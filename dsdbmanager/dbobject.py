import time
import typing
import toolz
import inspect
import functools
import warnings
import numpy as np
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc as exc
import sqlalchemy.sql.dml as dml

from .mssql_ import Mssql
from .mysql_ import Mysql
from .oracle_ import Oracle
from .teradata_ import Teradata
from .snowflake_ import Snowflake
from sqlalchemy.engine import reflection
from .configuring import ConfigFilesManager
from .utils import d_frame, inspect_table, filter_maker
from .constants import FLAVORS_FOR_CONFIG, CACHE_SIZE, CHUNK_SIZE
from .exceptions_ import (
    BadArgumentType, OperationalError, NoSuchColumn, MissingFlavor, NotImplementedFlavor,
    EmptyHostFile
)

host_type = typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]
update_key_type = typing.Union[typing.Tuple[str, ...], typing.Dict[str, str]]
table_middleware_type = typing.Callable[..., typing.Tuple[np.ndarray, typing.Tuple[str, ...]]]
connection_object_type = typing.Union[
    Oracle,
    Teradata,
    Mysql,
    Mssql,
    Snowflake
]


def util_function(table_name: str, engine: sa.engine.base.Engine, schema: str) -> sa.Table:
    """

    :param table_name: a table name. It must be a table in the schema of the engine
    :param engine: the sqlalchemy engine for the database
    :param schema: a schema of interest - None if default schema of database is ok
    :return: the sqlalchemy Table type for the table name provided
    """
    try:
        return sa.Table(table_name, sa.MetaData(engine, schema=schema), autoload=True)
    except exc.NoSuchTableError as e:
        raise e


def insert_into_table(df: pd.DataFrame, table_name: str, engine: sa.engine.Engine, schema: str) -> int:
    """

    :param df: a dataframe with same column names as those in the database table
    :param table_name: a table name as in util_function
    :param engine: the sqlalchemy engine for the database
    :param schema: a schema of interest - None if default schema of database is ok
    :return: the number of records inserted
    """

    # get the table
    tbl = util_function(table_name, engine, schema)

    # change all nan to None
    groups = toolz.partition_all(CHUNK_SIZE, df.where(pd.notnull(df), None).to_dict(orient='records'))

    # insert
    count, last_successful_insert = 0, None
    for group in groups:
        try:
            result = engine.execute(tbl.insert(), group)
            last_successful_insert = group[-1]
            count += result.rowcount
        except exc.OperationalError as _:
            "Try Again"
            time.sleep(2)

            try:
                result = engine.execute(tbl.insert(), group)
                last_successful_insert = group[-1]
                count += result.rowcount
            except exc.OperationalError as e:
                raise OperationalError(f"Failed to insert records. Last successful{last_successful_insert}", e)

    return count


def update_on_table(df: pd.DataFrame, keys: update_key_type, values: update_key_type, table_name: str,
                    engine: sa.engine.base.Engine, schema: str) -> int:
    """

    :param df: a dataframe with data tha needs to be updated. Must have columns to be used as key and some for values
    :param keys: the set of columns to use as key, i.e. update when matched
    :param values: the set of columns to update, i.e. set when matched
    :param table_name: a table name as in util_function
    :param engine: the sqlalchemy engine for the database
    :param schema: a schema of interest - None if default schema of database is ok
    :return: the number of records updated
    """

    # get table
    tbl = util_function(table_name, engine, schema)

    # change nan to None, make sure columns are modified so that we can easily bindparam
    df_ = df.copy()
    df_.columns = [f"{el.lower()}_updt" for el in df_.columns]
    groups = toolz.partition_all(CHUNK_SIZE, df_.where(pd.notnull(df_), None).to_dict(orient='records'))

    if not isinstance(keys, tuple) and not isinstance(keys, dict):
        raise BadArgumentType("keys and values must either be both tuples or both dicts", None)

    # create where clause, and update statement
    update_statement: dml.Update
    if isinstance(keys, tuple):
        if not isinstance(values, tuple):
            raise BadArgumentType("keys and values must either be both tuples or both dicts", None)

        where = [tbl.c[el] == sa.bindparam(f"{el.lower()}_updt") for el in keys]
        update_statement = tbl.update().where(sa.and_(*where)).values(
            dict((a, sa.bindparam(f"{a.lower()}_updt")) for a in values)
        )

    if isinstance(keys, dict):
        if not isinstance(values, dict):
            raise BadArgumentType("keys and values must either be both tuples or both dicts", None)
        where = [tbl.c[k] == sa.bindparam(f"{v.lower()}_updt") for k, v in keys.items()]
        update_statement = tbl.update().where(sa.and_(*where)).values(
            dict((k, sa.bindparam(f"{v.lower()}_updt")) for k, v in values.items())
        )

    # update
    count, last_successful_update = 0, None
    for group in groups:
        try:
            result = engine.execute(update_statement, group)
            last_successful_update = group[-1]
            count += result.rowcount
        except exc.OperationalError as _:
            # try again
            time.sleep(2)

            try:
                result = engine.execute(update_statement, group)
                last_successful_update = group[-1]
                count += result.rowcount
            except exc.OperationalError as e:
                raise OperationalError(f"Failed to update records. Last successful update: {last_successful_update}", e)

    return count


def table_middleware(engine: sa.engine.base.Engine, table: str, schema: str = None):
    """
    This does not directly look for the tables; it simply gives a function that can be used to specify
    number of rows and columns etc. When this function is evaluated, it returns a function that holds the context.
    That function has the table name, the schema and engine. It then knows what to query once it is called.

    :param engine: the sqlalchemy engine for the database
    :param table: a table name as in util_function
    :param schema: a schema of interest - None if default schema of database is ok
    :return: a function that when called, pulls data from the database table specified with 'table' arg
    """

    @d_frame
    @functools.lru_cache(CACHE_SIZE)
    def wrapped(
            rows: int = None,
            columns: typing.Tuple[str, ...] = None,
            **kwargs
    ) -> typing.Tuple[np.ndarray, typing.Tuple[str, ...]]:
        """

        :param rows: number of rows of data to pull
        :param columns: set of columns to pull
        :param kwargs: column to filter
        :return:
        """

        tbl = util_function(table, engine, schema)

        # query
        tbl_cols = [el.name for el in tbl.columns]
        if columns is None:
            query = sa.select([tbl])
        else:
            # check if all columns are in table
            not_in_table = set(columns) - set(tbl_cols)
            if not_in_table == set(columns):
                raise NoSuchColumn(f"None of the columns [{', '.join(sorted(columns))}] are in table {table}", None)

            if len(not_in_table) > 0:
                warnings.warn(f"Columns [{', '.join(sorted(not_in_table))}] are not in table {table}")

            tbl_cols = [el for el in columns if el in tbl_cols]
            query = sa.select([tbl.c[col] for col in tbl_cols])

        if kwargs:
            filters = [filter_maker(tbl, el, val) for el, val in kwargs.items()]
            query = query.where(sa.and_(*filters))

        # execute
        results = engine.execute(query)

        # fetch
        if rows is not None:
            array = results.fetchmany(rows)
        else:
            array = results.fetchall()

        results.close()

        # return dataframe
        arr, cols = np.array(array), tuple(tbl_cols)
        arr.flags.writeable = False
        return arr, cols

    return wrapped


class DbMiddleware(object):
    """
    This is the main class that is wrapped around the sqlalchemy engines

    Assume I have two tables, 'table_1' and 'table 2' in my default schema for an engine

    >>> dbobject = DbMiddleware(engine, False, None)
    >>> dbobject.sqlalchemy_engine.table_names()
    ['table_1', 'table 2']

    I can access the tables as they are properties or methods rather

    >>> dbobject.table1
    >>> dbobject['table 2']  # because it is not possible to use the . notation here


    But these do not do anything, in fact they are all just functions that I can call

    >>> dbobject.table1(rows=10)  # to get  the first 10 rows
    >>> dbobject['table 2'](rows=100, columns=('column', 'column with space'))  # to only get the specified columns

    I can also filter my data.

    Say I want column_3 in table1 to be equal to 'some_value'

    >>> dbobject.table1(column_3='some_value')

    If I want to get data only when column_3 is either 'some_value' or 'other_value'

    >>> dbobject.table1(column_3=('some_value', 'other_value'))  # here I pass a tuple instead of a single value

    tuples are used all around simply because we cache the result of these methods i.e. the dataframes

    Say I had a column name that had spaces and I couldn't just do what I did above, I could do this

    >>> dbobject.table1(**{'column with space': 'some_value'})  # simply unpacking the dictionary at execution time

    All those methods to pull data are **table_middleware** functions already evaluated at engine,
    table name and schema level.

    Bonus

    Get Metadata on your table

    >>> dbobject._metadata.table1()
    """

    def __init__(self, engine: sa.engine.Engine, connect_only: bool, schema: str = None):
        self.sqlalchemy_engine = engine

        if not connect_only:
            inspection = reflection.Inspector.from_engine(self.sqlalchemy_engine)
            views = inspection.get_view_names(schema=schema)
            tables = inspection.get_table_names(schema=schema)

            if not (tables + views):
                pass
            self._metadata = TableMeta(self.sqlalchemy_engine, schema, tables + views)
            self._insert = TableInsert(self.sqlalchemy_engine, schema, tables + views)
            self._update = TableUpdate(self.sqlalchemy_engine, schema, tables + views)

            for table in tables + views:
                self.__setattr__(table, table_middleware(self.sqlalchemy_engine, table, schema=schema))

    def __getitem__(self, item):
        return self.__dict__[item]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sqlalchemy_engine.dispose()
        properties = map(toolz.first, inspect.getmembers(self))
        methods_only = map(toolz.first, inspect.getmembers(self, inspect.ismethod))
        attributes = filter(lambda x: not x.startswith('__'), set(properties) - set(methods_only))
        for attribute in attributes:
            delattr(self, attribute)


@toolz.curry
def db_middleware(config_manager: ConfigFilesManager, flavor: str, db_name: str,
                  connection_object: connection_object_type, config_schema: str, connect_only: bool,
                  schema: str = None, **engine_kwargs) -> DbMiddleware:
    """
    Try connecting to the database. Write credentials on success. Using a function only so that the connection
    is only attempted when function is called.
    :param config_manager: a configuration manager to deal with host files etc
    :param flavor: the sql flavor/dialect where the database lies
    :param db_name: database name provided when adding database
    :param connection_object: one of the connectors from the likes of myql_.py to create engine
    :param config_schema: the schema provided when adding database
    :param connect_only: True if all we want is connect and not inspect for tables or views
    :param schema: if user wants to specify a different schema than the one supplied when adding database
    :param engine_kwargs: engine arguments, like echo, or warehouse, schema and role for snowflake
    :return:
    """

    username, password = config_manager.read_credentials(flavor, db_name)
    write_credentials = True

    if username is None or password is None:
        username, password = config_manager.ask_credentials()
    else:
        write_credentials = False

    engine: sa.engine.base.Engine = connection_object.create_engine(
        config_manager.encrypt_decrypt(username, encrypt=False).decode("utf-8"),
        config_manager.encrypt_decrypt(password, encrypt=False).decode("utf-8"),
        **engine_kwargs
    )

    try:
        engine.connect().close()
    except exc.DatabaseError as e:
        raise e

    if write_credentials:
        config_manager.write_credentials(flavor, db_name, username, password)

    if not schema:
        schema = config_schema

    # technically when connect_only is True, schema should not matter

    middleware = DbMiddleware(engine, connect_only, schema)
    return middleware


class DsDbManager(object):
    """
    oracle = DsDbManager('oracle') # will create the object with databases as properties/methods
    oracle.somedatabase  # a function to call with <connect_only, schema>
    -or-
    oracle['some data base']
    """

    def __init__(self, flavor: str, config_file_manager: ConfigFilesManager = None):
        if flavor.lower() not in FLAVORS_FOR_CONFIG:
            raise NotImplementedFlavor(
                f"Invalid flavor: expected one of {', '.join(FLAVORS_FOR_CONFIG)}, got {flavor}",
                None
            )

        self._flavor = flavor
        self._config_file_manager = ConfigFilesManager() if config_file_manager is None else config_file_manager
        self._host_dict = self._config_file_manager.get_hosts()

        if not self._host_dict:
            raise EmptyHostFile("Host file is empty", None)

        if flavor not in self._host_dict:
            raise MissingFlavor(f"No databases for {flavor}", None)

        # available databases
        self._available_databases = list(self._host_dict.get(flavor).keys())
        self._schemas_per_databases = [
            self._host_dict.get(flavor).get(database).get('schema', None)
            for database in self._available_databases
        ]

        # TODO: use schema provided by user if any. This will probably involve checking host dictionary
        for db_name, config_schema in zip(self._available_databases, self._schemas_per_databases):
            self.__setattr__(
                db_name,
                db_middleware(
                    self._config_file_manager,
                    self._flavor,
                    db_name,
                    self._connection_object_creator(db_name),
                    config_schema
                )
            )

    def _connection_object_creator(self, db_name: str):
        if self._flavor.lower() == 'oracle':
            return Oracle(db_name, self._host_dict)

        if self._flavor.lower() == 'teradata':
            return Teradata(db_name, self._host_dict)

        if self._flavor.lower() == 'mssql':
            return Mssql(db_name, self._host_dict)

        if self._flavor.lower() == 'mysql':
            return Mysql(db_name, self._host_dict)

        if self._flavor.lower() == 'snowflake':
            return Snowflake(db_name, self._host_dict)

    def __getitem__(self, item):
        return self.__dict__[item]


class TableMeta(object):
    """
    We have to create distinct functions for each table. Once the function is called, the metadata is provided
    """

    def __init__(self, engine: sa.engine.base.Engine, schema: str, tables: typing.Tuple[str, ...]):
        for table in tables:
            def meta_function(t: str = table):
                tbl = util_function(t, engine, schema)
                return inspect_table(tbl)

            self.__setattr__(table, meta_function)


class TableInsert(object):
    """
    distinct functions for each table
    """

    def __init__(self, engine: sa.engine.base.Engine, schema: str, tables: typing.Tuple[str, ...]):
        for table in tables:
            insert_function = functools.partial(insert_into_table, engine=engine, schema=schema)

            def insert_func(df: pd.DataFrame, t: str = table):
                """

                :param df:
                :param t:
                :return:
                """
                return insert_function(df, t)

            self.__setattr__(table, insert_func)


class TableUpdate(object):
    """
    distinct functions for each table
    """

    def __init__(self, engine: sa.engine.base.Engine, schema: str, tables: typing.Tuple[str, ...]):
        for table in tables:
            update_function = functools.partial(update_on_table, engine=engine, schema=schema)

            def update_func(df: pd.DataFrame, keys: update_key_type, values: update_key_type, t: str = table):
                """

                :param df:
                :param keys:
                :param values:
                :param t:
                :return:
                """
                return update_function(df, keys, values, t)

            self.__setattr__(table, update_func)
