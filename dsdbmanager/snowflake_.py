import typing
import sqlalchemy as sa
from .configuring import ConfigFilesManager
from .exceptions_ import MissingFlavor, MissingDatabase, MissingPackage

host_type = typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]


class Snowflake:
    def __init__(self, db_name: str, host_dict: host_type = None):
        """

        :param db_name: database name
        :param host_dict: optional database info with host, ports etc
        """
        self.db_name = db_name
        self.host_dict: host_type = ConfigFilesManager().get_hosts() if not host_dict else host_dict

        if not self.host_dict or 'snowflake' not in self.host_dict:
            raise MissingFlavor("No databases available for snowflake", None)

        self.host_dict = self.host_dict.get('snowflake').get(self.db_name, {})

        if not self.host_dict:
            raise MissingDatabase(f"{self.db_name} has not been added for snowflake", None)

    def create_engine(self, user: str = None, pwd: str = None, **kwargs):
        """

        :param user: username
        :param pwd: password
        :param kwargs: for compatibility/additional sqlalchemy create_engine kwargs or things like role, warehouse etc.
        :return: sqlalchemy engine
        """
        try:
            from snowflake.sqlalchemy import URL
        except ImportError as e:
            raise MissingPackage("You need the snowflake-sqlalchemy package to initiate connection", e)

        host = self.host_dict.get('host')

        url = URL(
            account=host,
            user=user,
            password=pwd,
            database=self.db_name,
            **kwargs
        )

        # TODO - find a way to identify kwrags consumed by URL and pull them out of kwargs and pass the rest below

        return sa.create_engine(url)
