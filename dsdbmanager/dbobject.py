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

from .mssql import Mssql
from .mysql import Mysql
from .oracle import Oracle
from .teradata import Teradata
from sqlalchemy.engine import reflection
from .configuring import ConfigFilesManager
from .utils import d_frame, inspect_table, filter_maker
from .constants import FLAVORS_FOR_CONFIG, CACHE_SIZE, CHUNK_SIZE

host_type = typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]
update_key_type = typing.Union[typing.Tuple[str, ...], typing.Dict[str, str]]
table_middleware_type = typing.Callable[..., typing.Tuple[np.ndarray, typing.Tuple[str, ...]]]
connection_object_type = typing.Union[
    Oracle,
    Teradata,
    Mysql,
    Mssql
]


def util_function(table_name: str, engine: sa.engine.base.Engine, schema: str) -> sa.Table:
    """

    :param table_name:
    :param engine:
    :param schema:
    :return:
    """
    try:
        return sa.Table(table_name, sa.MetaData(engine, schema=schema), autoload=True)
    except exc.NoSuchTableError as e:
        raise e


def insert_into_table(df: pd.DataFrame, table_name: str, engine: sa.engine.Engine, schema: str) -> int:
    """

    :param df:
    :param table_name:
    :param engine:
    :param schema:
    :return:
    """

    # get the table
    tbl = util_function(table_name, engine, schema)

    # change all nan to None
    groups = toolz.partition_all(CHUNK_SIZE, df.where(pd.isnull(df), None).to_dict(orient='records'))

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
                raise Exception(f"Failed to insert records. Last successfull{last_successful_insert}", e)

    return count


def update_on_table(df: pd.DataFrame, keys: update_key_type, values: update_key_type, table_name: str,
                    engine: sa.engine.base.Engine, schema: str) -> int:
    """

    :param df:
    :param keys:
    :param values:
    :param table_name:
    :param engine:
    :param schema:
    :return:
    """

    # get table
    tbl = util_function(table_name, engine, schema)

    # change nan to None, make sure columns are modified so that we can easily bindparam
    df_ = df.copy()
    df_.columns = [f"{el.lower()}_updt" for el in df_.columns]
    groups = toolz.partition_all(CHUNK_SIZE, df_.where(pd.isnull(df_), None).to_dict(orient='records'))

    # create where clause, and update statement
    update_statement: dml.Update
    if isinstance(keys, tuple):
        if not isinstance(values, tuple):
            raise Exception("keys and values must either be both tuples or both dicts")

        where = [tbl.c[el] == sa.bindparam(f"{el.lower()}_updt") for el in keys]
        update_statement = tbl.update().where(sa.and_(*where)).values(
            dict((a, sa.bindparam(f"{a.lower()}_updt")) for a in values)
        )

    if isinstance(keys, dict):
        if not isinstance(values, dict):
            raise Exception("keys and values must either be both tuples or both dicts")
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
                raise Exception(f"Failed to update records. Lase succesful update: {last_successful_update}", e)

    return count


def table_middleware(engine: sa.engine.base.Engine, table: str, schema: str = None):
    """
    This does not directly look for the tables; it simply gives a function that can be used to specify
    number of rows and columns etc.
    :param engine:
    :param table:
    :param schema:
    :return:
    """

    @d_frame
    @functools.lru_cache(CACHE_SIZE)
    def wrapped(
            rows: int = None,
            columns: typing.Tuple[str, ...] = None,
            **kwargs
    ) -> typing.Tuple[np.ndarray, typing.Tuple[str, ...]]:
        """

        :param rows:
        :param columns:
        :param kwargs:
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
                raise Exception(f"None of the columns [{', '.join(columns)}] are in table {table}")

            if len(not_in_table) > 0:
                warnings.warn(f"Columns [{''.join(not_in_table)}] are not in table {table}")

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


@toolz.curry
def db_middleware(config_manager: ConfigFilesManager, flavor: str, db_name: str,
                  connection_object: connection_object_type, connect_only: bool, schema: str):
    """
    Try connecting to the database. Write credentials on success. Using a function only so that the connection
    is only attempted when function is called.
    :param config_manager:
    :param flavor:
    :param db_name:
    :param connection_object:
    :param connect_only:
    :param schema:
    :return:
    """

    username, password = config_manager.read_credentials(flavor, db_name)

    if username is None or password is None:
        write_credentials = True
        username, password = config_manager.ask_credentials()
    else:
        write_credentials = False

    engine: sa.engine.base.Engine = connection_object.create_engine(
        config_manager.encrypt_decrypt(username, encrypt=False).decode("utf-8"),
        config_manager.encrypt_decrypt(password, encrypt=False).decode("utf-8")
    )

    try:
        engine.connect().close()
    except exc.DatabaseError as e:
        raise e

    if write_credentials:
        config_manager.write_credentials(flavor, db_name, username, password)

    middleware = DbMiddleware(engine, connect_only, schema)
    return middleware


class DbMiddleware(object):
    def __init__(self, engine, connect_only, schema):
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


class DsDbManager(object):
    """
    oracle = DsDbManager('oracle') # will create the object with databases as properties/methods
    oracle.somedatabase  # a function to call with <connect_only, schema>
    -or-
    oracle['some data base']
    """

    def __init__(self, flavor: str):
        if flavor.lower() not in FLAVORS_FOR_CONFIG:
            raise Exception(f"Invalid flavor: expected one of {', '.join(FLAVORS_FOR_CONFIG)}, got {flavor}")

        self._flavor = flavor
        self._config_file_manager = ConfigFilesManager()
        self._host_dict = self._config_file_manager.get_hosts()

        if not self._host_dict:
            raise Exception("Host file is empty. Consider adding some databases")

        # available databases
        self.available_databases = list(self._host_dict.get(flavor).keys())

        # TODO: use schema provided by user if any. This will probably involve checking host dictionary
        for db_name in self.available_databases:
            self.__setattr__(
                db_name,
                db_middleware(
                    self._config_file_manager,
                    self._flavor,
                    db_name,
                    self.connection_object_creator(db_name)
                )
            )

    def connection_object_creator(self, db_name: str):
        if self._flavor.lower() == 'oracle':
            return Oracle(db_name, self._host_dict)

        if self._flavor.lower() == 'teradata':
            return Teradata(db_name, self._host_dict)

        if self._flavor.lower() == 'mssql':
            return Mssql(db_name, self._host_dict)

        if self._flavor.lower() == 'mysql':
            return Mysql(db_name, self._host_dict)

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
