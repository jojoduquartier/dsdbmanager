import typing
import sqlalchemy as sa
from .configuring import ConfigFilesManager
from .exceptions_ import MissingFlavor, MissingDatabase, MissingPackage

host_type = typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]


class Mysql:
    def __init__(self, db_name: str, host_dict: host_type = None):
        """

        :param db_name: database name
        :param host_dict: optional database info with host, ports etc
        """
        self.db_name = db_name
        self.host_dict: host_type = ConfigFilesManager(
        ).get_hosts() if not host_dict else host_dict

        if not self.host_dict or 'mysql' not in self.host_dict:
            raise MissingFlavor("No databases available for mysql", None)

        self.host_dict = self.host_dict.get('mysql').get(self.db_name, {})

        if not self.host_dict:
            raise MissingDatabase(
                f"{self.db_name} has not been added for mysql", None)

    def create_engine(self, user: str = None, pwd: str = None, **kwargs):
        """

        :param user: username
        :param pwd: password
        :param kwargs: for compatibility/additional sqlalchemy create_engine kwargs
        :return: sqlalchemy engine
        """
        try:
            import pymysql
        except ImportError as e:
            raise MissingPackage(
                "You need the PyMySQL package to initiate connection", e
            )

        host = self.host_dict.get('host')

        if 'port' not in self.host_dict:
            if (
                'use_dbname_to_connect' in self.host_dict and
                not self.host_dict['use_dbname_to_connect']
            ):
                return sa.create_engine(f'mysql+pymysql://{user}:{pwd}@{host}:/', **kwargs)
            else:
                return sa.create_engine(f'mysql+pymysql://{user}:{pwd}@{host}:/{self.db_name}', **kwargs)

        port = self.host_dict.get('port')
        if (
            'use_dbname_to_connect' in self.host_dict and
            not self.host_dict['use_dbname_to_connect']
        ):
            return sa.create_engine(f'mysql+pymysql://{user}:{pwd}@{host}:{port}/', **kwargs)
        else:
            return sa.create_engine(f'mysql+pymysql://{user}:{pwd}@{host}:{port}/{self.db_name}', **kwargs)
