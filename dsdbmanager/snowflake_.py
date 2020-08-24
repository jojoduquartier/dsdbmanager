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
        self.host_dict: host_type = ConfigFilesManager(
        ).get_hosts() if not host_dict else host_dict

        if not self.host_dict or 'snowflake' not in self.host_dict:
            raise MissingFlavor("No databases available for snowflake", None)

        self.host_dict = self.host_dict.get('snowflake').get(self.db_name, {})

        if not self.host_dict:
            raise MissingDatabase(
                f"{self.db_name} has not been added for snowflake", None)

    def create_engine(
        self,
        user: str = None,
        pwd: str = None,
        raw_connection=False,
        **kwargs
    ):
        """

        :param user: username
        :param pwd: password
        :param raw_connection: true or false
        :param kwargs: for compatibility/additional sqlalchemy create_engine kwargs or things like role, warehouse etc.
        :return: sqlalchemy engine if raw_connection is false or snowflake connector when true
        """
        try:
            from snowflake.sqlalchemy import URL
            from snowflake.connector import connect
        except ImportError as e:
            raise MissingPackage(
                "You need the snowflake-sqlalchemy and snowflake packages to initiate connection", e)

        url = dict(
            self.host_dict.items(),
            user=user,
            password=pwd
        )

        # TODO - find a way to identify kwrags consumed by URL and pull them out of kwargs and pass the rest below
        if raw_connection:
            # this could be obtained from the raw_connection method from sqlalchemy
            return connect(**url)

        return sa.create_engine(URL(**url), **kwargs)
