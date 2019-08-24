import typing
import sqlalchemy as sa
from .configuring import ConfigFilesManager

host_type = typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]


class Mssql:
    def __init__(self, db_name: str, host_dict: host_type = None):
        self.db_name = db_name
        self.host_dict: host_type = ConfigFilesManager().get_hosts() if not host_dict else host_dict

        if not self.host_dict:
            raise Exception("No databases available")

        self.host_dict = self.host_dict.get('mssql').get(self.db_name, {})

        if not self.host_dict:
            raise Exception(f"{self.db_name} has not been added")

    def create_engine(self, user: str = None, pwd: str = None, **kwargs):
        """

        :param user:
        :param pwd:
        :param kwargs:
        :return:
        """
        try:
            import pymssql
        except ImportError as e:
            raise e

        host = self.host_dict.get('host')

        if 'port' not in self.host_dict:
            return sa.create_engine(f'mssql+pymsql://{user}:{pwd}@{host}/{self.db_name}')

        port = self.host_dict.get('port')
        return sa.create_engine(f'mssql+pymsql://{user}:{pwd}@{host}{port}/{self.db_name}')
