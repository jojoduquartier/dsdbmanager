import typing
import sqlalchemy as sa
from .configuring import ConfigFilesManager

host_type = typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]


class Oracle:
    def __init__(self, db_name: str, host_dict: host_type = None):
        self.host_dict: host_type = ConfigFilesManager().get_hosts() if not host_dict else host_dict

        # if the host file is empty raise an exception
        if not self.host_dict or 'oracle' not in self.host_dict:
            raise Exception("No databases available")

        self.host_dict = self.host_dict.get('oracle').get(db_name, {})

        # if the database has not been added there is nothing we can do
        if not self.host_dict:
            raise Exception(f"{db_name} has not been added")

    def create_engine(self, user: str = None, pwd: str = None, **kwargs):
        """

        :param user:
        :param pwd:
        :param kwargs: for compatibility/additional sqlalchemy create_engine kwargs
        :return:
        """
        try:
            from cx_Oracle import makedsn
        except ImportError as e:
            raise e

        host = self.host_dict.get('host')
        port = self.host_dict.get('port', 1521)
        sid = self.host_dict.get('sid', None)

        if 'service_name' in self.host_dict:
            dsn = makedsn(host, port, service_name=self.host_dict.get('service_name'))
        else:
            dsn = makedsn(host, port, sid=sid)

        return sa.create_engine(f'oracle://{user}:{pwd}@{dsn}', **kwargs)
