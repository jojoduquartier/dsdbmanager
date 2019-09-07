import typing
import sqlalchemy as sa
from .configuring import ConfigFilesManager

host_type = typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]


class Mysql:
    def __init__(self, db_name: str, host_dict: host_type = None):
        self.db_name = db_name
        self.host_dict: host_type = ConfigFilesManager().get_hosts() if not host_dict else host_dict

        if not self.host_dict or 'mysql' not in self.host_dict:
            raise Exception("No databases available")

        self.host_dict = self.host_dict.get('mysql').get(self.db_name, {})

        if not self.host_dict:
            raise Exception(f"{self.db_name} has not been added")

    def create_engine(self, user: str = None, pwd: str = None, **kwargs):
        """

        :param user: username
        :param pwd: password
        :param kwargs: for compatibility/additional sqlalchemy create_engine kwargs
        :return:
        """
        try:
            import pymysql
        except ImportError as e:
            raise e

        host = self.host_dict.get('host')

        if 'port' not in self.host_dict:
            return sa.create_engine(f'mysql+pymysql://{user}:{pwd}@{host}:/{self.db_name}', **kwargs)

        port = self.host_dict.get('port')
        return sa.create_engine(f'mysql+pymysql://{user}:{pwd}@{host}:{port}/{self.db_name}', **kwargs)
