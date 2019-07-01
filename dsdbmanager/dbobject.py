import json
import time
import typing
import toolz
import inspect
import functools
import numpy as np
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc as exc
from sqlalchemy.engine import reflection
from .utils import d_frame, inspect_table, filter_maker, complex_filter_maker
from .constants import HOST_PATH, FLAVORS, CACHE_SIZE, CHUNK_SIZE
from .configuring import Configure

host_type = typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]
update_key_type = typing.Union[typing.Tuple[str, ...], typing.Dict[str, str]]
table_middleware_type = typing.Callable[..., typing.Tuple[np.ndarray, typing.Tuple[str, ...]]]


class Connector(object):
    def __init__(self):
        if not HOST_PATH.exists():
            raise Exception("No Host File Available")

        with open(HOST_PATH, 'r') as fp:
            host_data = json.load(fp)

        # if the host file is empty
        if not host_data:
            raise Exception("Empty Host File")

        for flavor in FLAVORS:
            self.__setattr__(flavor, factory(flavor))

    def __getitem__(self, key):
        return getattr(self, key)


def factory(flavor: str):
    """

    Args:
        flavor:

    Returns:

    """

    class FromEngine(object):
        def __init__(self, db: str, pre_made_engine: sa.engine.base.Engine):
            self.__setattr__(db, db_middleware(None, db, pre_made_engine=pre_made_engine))

    class Oracle(object):
        def __init__(self):
            with open(HOST_PATH, 'r') as fp:
                host_dict = json.load(fp)

            for db in host_dict.get('oracle'):
                self.__setattr__(db, db_middleware('oracle', db, host_dict=host_dict))

        @classmethod
        def create_engine(cls, host: str = None, port: int = 1521, sid: str = None, user: str = None, pwd: str = None,
                          **kwargs):
            try:
                from cx_Oracle import makedsn
            except ImportError as e:
                raise e

            if 'service_name' in kwargs:
                dsn = makedsn(host, port, service_name=kwargs.get('service_name'))
            else:
                dsn = makedsn(host, port, sid=sid)

            return sa.create_engine(f'oracle://{user}:{pwd}@{dsn}')

    class Teradata(object):
        def __init__(self):
            with open(HOST_PATH, 'r') as fp:
                host_dict = json.load(fp)

            for db in host_dict.get('teradata'):
                self.__setattr__(db, db_middleware('teradata', db, host_dict=host_dict))

        @classmethod
        def create_engine(cls, host: str = None, user: str = None, pwd: str = None, **kwargs):
            try:
                import sqlalchemy_teradata
            except ImportError as e:
                raise e

            return sa.create_engine(f'teradata://{user}:{pwd}@{host}')

    class Mssql(object):
        def __init__(self):
            with open(HOST_PATH, 'r') as fp:
                host_dict = json.load(fp)

            for db in host_dict.get('mssql'):
                self.__setattr__(db, db_middleware('mssql', db, host_dict=host_dict))

        @classmethod
        def create_engine(cls, db: str = None, host: str = None, user: str = None, pwd: str = None, **kwargs):
            try:
                import pymssql
            except ImportError as e:
                raise e

            if 'port' not in kwargs:
                return sa.create_engine(f'mssql+pymsql://{user}:{pwd}@{host}/{db}')

            port = kwargs.get('port')
            return sa.create_engine(f'mssql+pymsql://{user}:{pwd}@{host}{port}/{db}')

    class Mysql(object):
        def __init__(self):
            with open(HOST_PATH, 'r') as fp:
                host_dict = json.load(fp)

            for db in host_dict.get('mysql'):
                self.__setattr__(db, db_middleware('mysql', db, host_dict=host_dict))

        @classmethod
        def create_engine(cls, db: str = None, host: str = None, user: str = None, pwd: str = None, **kwargs):
            try:
                import pymysql
            except ImportError as e:
                raise e

            if 'port' not in kwargs:
                return sa.create_engine(f'mysql+pymysql://{user}:{pwd}@{host}:/{db}')

            port = kwargs.get('port')
            return sa.create_engine(f'mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}')

    if flavor.lower().strip() == 'oracle':
        return Oracle

    if flavor.lower().strip() == 'teradata':
        return Teradata

    if flavor.lower().strip() == 'mssql':
        return Mssql

    if flavor.lower().strip() == 'mysql':
        return Mysql

    if flavor.lower().strip() == 'fromengine':
        return FromEngine


def db_middleware(flavor: str, db: str, host_dict: host_type = {}, pre_made_engine: sa.engine.base.Engine = None):
    """

    Args:
        flavor:
        db:
        host_dict:
        pre_made_engine:

    Returns:

    """

    def util_function(table_: str, engine_: sa.engine.base.Engine, schema_: str) -> sa.Table:
        """

        Args:
            table_:
            engine_:
            schema_:

        Returns:

        """
        try:
            return sa.Table(table_, sa.MetaData(engine_, schema=schema_), autoload=True)
        except exc.NoSuchTableError as e:
            raise e

    def insert_into_table(df: pd.DataFrame, table_: str, engine_: sa.engine.Engine, schema_: str) -> int:
        """

        Args:
            df:
            table_:
            engine_:
            schema_:

        Returns:

        """

        # get the table
        tbl = util_function(table_, engine_, schema_)

        # change all nan to None
        groups = toolz.partition_all(CHUNK_SIZE, df.where(pd.isnull(df), None).to_dict(orient='records'))

        # insert
        count, last_successful_insert = 0, None
        for group in groups:
            try:
                result = engine_.execute(tbl.insert(), group)
                last_successful_insert = group[-1]
                count += result.rowcount
            except exc.OperationalError as _:
                "Try Again"
                time.sleep(2)

                try:
                    result = engine_.execute(tbl.insert(), group)
                    last_successful_insert = group[-1]
                    count += result.rowcount
                except exc.OperationalError as e:
                    raise Exception(f"Failed to insert records. Last successfull{last_successful_insert}", e)

        return count

    def update_on_table(df: pd.DataFrame, keys: update_key_type, values: update_key_type, table_: str,
                        engine_: sa.engine.base.Engine, schema_: str) -> int:
        """

        Args:
            df:
            keys:
            values:
            table_:
            engine_:
            schema_:

        Returns:

        """

        # get table
        tbl = util_function(table_, engine_, schema_)

        # change nan to None, make sure columns are modified so that we can easily bindparam
        df_ = df.copy()
        df_.columns = [f"{el.lower()}_updt" for el in df_.columns]
        groups = toolz.partition_all(CHUNK_SIZE, df_.where(pd.isnull(df_), None).to_dict(orient='records'))

        # create where clause, and update statement
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
                result = engine_.execute(update_statement, group)
                last_successful_update = group[-1]
                count += result.rowcount
            except exc.OperationalError as _:
                # try again
                time.sleep(2)

                try:
                    result = engine_.execute(update_statement, group)
                    last_successful_update = group[-1]
                    count += result.rowcount
                except exc.OperationalError as e:
                    raise Exception(f"Failed to update records. Lase succesful update: {last_successful_update}", e)

        return count

    def table_middleware(engine: sa.engine.base.Engine, table: str, schema: str = None) -> table_middleware_type:
        """

        Args:
            engine:
            table:
            schema:

        Returns:

        """

        @d_frame
        @functools.lru_cache(CACHE_SIZE)
        def wrapped(
                rows: int = None,
                columns: typing.Tuple[str, ...] = None,
                between: typing.Tuple[str, typing.Tuple[typing.Any, typing.Any]] = None,
                less_than: typing.Tuple[str, typing.Any] = None,
                less_than_or_equal: typing.Tuple[str, typing.Any] = None,
                greater_than: typing.Tuple[str, typing.Any] = None,
                greater_than_or_equal: typing.Tuple[str, typing.Any] = None,
                like: typing.Tuple[str, str] = None,
                not_like: typing.Tuple[str, str] = None,
                not_in: typing.Tuple[str, typing.Tuple[typing.Any, ...]] = None,
                **kwargs
        ) -> typing.Tuple[np.ndarray, typing.Tuple[str, ...]]:
            """

            Args:
                rows:
                columns:
                between:
                less_than:
                less_than_or_equal:
                greater_than:
                greater_than_or_equal:
                like:
                not_like:
                not_in:
                **kwargs:

            Returns:

            """

            tbl = util_function(table, engine, schema)

            # query
            tbl_cols = [el.name for el in tbl.columns]
            if columns is None:
                query = sa.select([tbl])
            else:
                # check if all columns are in table
                not_in_table = set(columns) - set(tbl_cols)
                if len(not_in_table) > 0:
                    pass

                tbl_cols = [el for el in columns if el in tbl_cols]
                query = sa.select([tbl.c[col] for col in tbl_cols])

            # TODO - we could check if the first item of each of this is a tuple
            # that way user could do: between = ((column1, value1, value2), (column2, value3, value4), ...)
            # then we just do complex_filter_maker(*something)

            if less_than is not None:
                query = query.where(complex_filter_maker(tbl, less_than, 'lt'))

            if less_than_or_equal is not None:
                query = query.where(complex_filter_maker(tbl, less_than_or_equal, 'le'))

            if greater_than is not None:
                query = query.where(complex_filter_maker(tbl, greater_than, 'gt'))

            if greater_than_or_equal is not None:
                query = query.where(complex_filter_maker(tbl, greater_than_or_equal, 'ge'))

            if between is not None:
                query = query.where(complex_filter_maker(tbl, between, 'bw'))

            if like is not None:
                query = query.where(complex_filter_maker(tbl, like, 'like'))

            if not_like is not None:
                query = query.where(complex_filter_maker(tbl, not_like, 'not_like'))

            if not_in is not None:
                query = query.where(complex_filter_maker(tbl, not_in, 'not_in'))

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

    def object_creation(table_schema: str = None, connect_only: bool = False):
        """

        Args:
            table_schema:
            connect_only:

        Returns:

        """

        class DbObject(object):
            def __setattr__(self, key, value):
                self.__dict__[key] = value

            def __getitem__(self, key):
                return getattr(self, key)

            def __init__(self, flavor: str, db: str, schema: str = table_schema):
                """

                Args:
                    flavor:
                    db:
                    schema:
                """

                self.configurer = Configure()
                creds = self.configurer.read_credentials(flavor, db)

                if pre_made_engine is None and creds is None:
                    user, pwd = self.configurer.ask_credentials()
                else:
                    if pre_made_engine is None:
                        user, pwd = creds

                if pre_made_engine is None:
                    connector = Connector()
                    cred_dict = host_dict.get(flavor).get(db)
                    cred_dict.update(
                        {
                            'user': self.configurer.encrypt_decrypt(user, False).decode('utf-8'),
                            'pwd': self.configurer.encrypt_decrypt(pwd, False).decode('utf-8')
                        }
                    )
                    self.sqlEngine = connector[flavor].create_engine(db=db, **cred_dict)
                else:
                    self.sqlEngine = pre_made_engine

                # test connection
                try:
                    self.sqlEngine.connect().close()

                    # write credentials when succeeded
                    if pre_made_engine is None and creds is None:
                        self.configurer.write_credentials(flavor, db, user, pwd)

                    self.flavor = flavor if pre_made_engine is None else pre_made_engine.dialect.name.lower()
                    if pre_made_engine is None:
                        self.schema = cred_dict.get('schema') if schema is None else schema
                    else:
                        self.schema = pre_made_engine.dialect.default_schema_name if schema is None else schema

                    if not connect_only:
                        insp = reflection.Inspector.from_engine(self.sqlEngine)
                        views = insp.get_view_names(schema=self.schema)
                        tables = insp.get_table_names(schema=self.schema)

                        if not (tables + views):
                            pass
                        self.m = TableMeta(self.sqlEngine, self.schema, tables + views)
                        self.x = TableInsert(self.sqlEngine, self.schema, tables + views)
                        self.u = TableUpdate(self.sqlEngine, self.schema, tables + views)

                        for table in tables + views:
                            self.__setattr__(table, table_middleware(self.sqlEngine, table, schema=self.schema))
                except exc.DatabaseError as e:
                    raise e

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                # dispose of engine
                self.sqlEngine.dispose()

                # remove properties
                properties = map(toolz.first, inspect.getmembers(self))
                methods_only = map(toolz.first, inspect.getmembers(self, inspect.ismethod))
                attributes = filter(lambda x: not x.startswith('__'), set(properties) - set(methods_only))
                for attribute in attributes:
                    delattr(self, attribute)

                self.merge = lambda: None
                self.execute_raw_sql = lambda: None

        class TableMeta(object):
            def __init__(self, engine_: sa.engine.base.Engine, schema_: str, tables: typing.Tuple[str, ...]):
                pass

        class TableInsert(object):
            def __init__(self, engine_: sa.engine.base.Engine, schema_: str, tables: typing.Tuple[str, ...]):
                pass

        class TableUpdate(object):
            def __init__(self, engine_: sa.engine.base.Engine, schema_: str, tables: typing.Tuple[str, ...]):
                pass
