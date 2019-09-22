import typing
import sqlalchemy as sa
from .configuring import ConfigFilesManager
from .exceptions_ import MissingFlavor, MissingDatabase, MissingPackage

host_type = typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]


class Mssql:
    def __init__(self, db_name: str, host_dict: host_type = None):
        """

        :param db_name: database name
        :param host_dict: optional database info with host, ports etc
        """
        self.db_name = db_name
        self.host_dict: host_type = ConfigFilesManager().get_hosts() if not host_dict else host_dict

        if not self.host_dict or 'mssql' not in self.host_dict:
            raise MissingFlavor("No databases available for mssql", None)

        self.host_dict = self.host_dict.get('mssql').get(self.db_name, {})

        if not self.host_dict:
            raise MissingDatabase(f"{self.db_name} has not been added for mssql", None)

    def create_engine(self, user: str = None, pwd: str = None, **kwargs):
        """

        :param user: username
        :param pwd: password
        :param kwargs: for compatibility/additional sqlalchemy create_engine kwargs
        :return: sqlalchemy engine
        """
        try:
            import pymssql
        except ImportError as e:
            raise MissingPackage("You need the pymssql package to initiate connection", e)

        host = self.host_dict.get('host')

        if 'port' not in self.host_dict:
            return sa.create_engine(f'mssql+pymssql://{user}:{pwd}@{host}/{self.db_name}', **kwargs)

        port = self.host_dict.get('port')
        return sa.create_engine(f'mssql+pymssql://{user}:{pwd}@{host}{port}/{self.db_name}', **kwargs)
