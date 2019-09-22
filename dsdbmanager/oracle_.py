import typing
import sqlalchemy as sa
from .configuring import ConfigFilesManager
from .exceptions_ import MissingFlavor, MissingDatabase, MissingPackage

host_type = typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]


class Oracle:
    def __init__(self, db_name: str, host_dict: host_type = None):
        """

        :param db_name: database name
        :param host_dict: optional database info with host, ports etc
        """
        self.host_dict: host_type = ConfigFilesManager().get_hosts() if not host_dict else host_dict

        # if the host file is empty raise an exception
        if not self.host_dict or 'oracle' not in self.host_dict:
            raise MissingFlavor("No databases available for oracle", None)

        self.host_dict = self.host_dict.get('oracle').get(db_name, {})

        # if the database has not been added there is nothing we can do
        if not self.host_dict:
            raise MissingDatabase(f"{db_name} has not been added for oracle", None)

    def create_engine(self, user: str = None, pwd: str = None, **kwargs):
        """

        :param user: username
        :param pwd: password
        :param kwargs: for compatibility/additional sqlalchemy create_engine kwargs
        :return: sqlalchemy engine
        """
        try:
            from cx_Oracle import makedsn
        except ImportError as e:
            raise MissingPackage("You need the cx_Oracle package to initiate connection", e)

        host = self.host_dict.get('host')
        port = self.host_dict.get('port', 1521)
        sid = self.host_dict.get('sid', None)

        if 'service_name' in self.host_dict:
            dsn = makedsn(host, port, service_name=self.host_dict.get('service_name'))
        else:
            dsn = makedsn(host, port, sid=sid)

        return sa.create_engine(f'oracle://{user}:{pwd}@{dsn}', **kwargs)
