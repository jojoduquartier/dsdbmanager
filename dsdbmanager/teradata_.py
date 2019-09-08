import typing
import sqlalchemy as sa
from .configuring import ConfigFilesManager

host_type = typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]


class Teradata:
    def __init__(self, db_name: str, host_dict: host_type = None):
        """

        :param db_name: database name
        :param host_dict: optional database info with host, ports etc
        """
        self.host_dict: host_type = ConfigFilesManager().get_hosts() if not host_dict else host_dict

        if not self.host_dict or 'teradata' not in self.host_dict:
            raise Exception("No databases available")

        self.host_dict = self.host_dict.get('teradata').get(db_name, {})

        if not self.host_dict:
            raise Exception(f"{db_name} has not been added")

    def create_engine(self, user: str = None, pwd: str = None, **kwargs):
        """

        :param user: username
        :param pwd: password
        :param kwargs: for compatibility/additional sqlalchemy create_engine kwargs
        :return: sqlalchemy engine
        """
        try:
            import sqlalchemy_teradata
        except ImportError as e:
            raise e

        host = self.host_dict.get('host')

        return sa.create_engine(f'teradata://{user}:{pwd}@{host}', **kwargs)
