import typing
import sqlalchemy as sa
from .configuring import ConfigFilesManager
from .exceptions_ import MissingFlavor, MissingDatabase, MissingPackage

host_type = typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]


class Teradata:
    def __init__(self, db_name: str, host_dict: host_type = None):
        """

        :param db_name: database name
        :param host_dict: optional database info with host, ports etc
        """
        self.host_dict: host_type = ConfigFilesManager().get_hosts() if not host_dict else host_dict

        if not self.host_dict or 'teradata' not in self.host_dict:
            raise MissingFlavor("No databases available for teradata", None)

        self.host_dict = self.host_dict.get('teradata').get(db_name, {})

        if not self.host_dict:
            raise MissingDatabase(f"{db_name} has not been added for teradata", None)

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
            raise MissingPackage("You need the sqlalchemy_teradata package to initiate connection", e)

        host = self.host_dict.get('host')

        return sa.create_engine(f'teradata://{user}:{pwd}@{host}', **kwargs)
