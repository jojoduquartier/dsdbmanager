import json
import typing
import numpy as np
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc as exc
from .constants import HOST_PATH, FLAVORS
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

    def __getitem__(self, item):
        return getattr(self, item)


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
        pass

    def update_on_table(df: pd.DataFrame, keys: update_key_type, values: update_key_type, table_: str,
                        engine_: sa.engine.base.Engine, schema_: str) -> int:
        pass

    def table_middleware(engine: sa.engine.base.Engine, table: str, schema: str = None) -> table_middleware_type:
        pass
