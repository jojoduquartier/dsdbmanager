import json
import click
import typing
import pathlib
import warnings
from cryptography.fernet import Fernet
from .exceptions_ import MissingFlavor, MissingDatabase
from .constants import HOST_PATH, CREDENTIAL_PATH, KEY_PATH, FLAVORS_FOR_CONFIG


class ConfigFilesManager(object):
    def __init__(
            self,
            host_path: pathlib.Path = HOST_PATH,
            credential_path: pathlib.Path = CREDENTIAL_PATH,
            key_path: pathlib.Path = KEY_PATH
    ):
        self.host_location: pathlib.Path = host_path
        self.credential_location: pathlib.Path = credential_path
        self.key_location: pathlib.Path = key_path

    @classmethod
    def generate_key(cls):
        return Fernet.generate_key()

    def get_hosts(self):
        try:
            with self.host_location.open('r') as f:
                hosts = json.load(f)

        except (OSError, json.JSONDecodeError) as _:
            hosts = {}

        return hosts

    def encrypt_decrypt(self, string: typing.Union[str, bytes], encrypt: bool) -> typing.Union[str, bytes]:
        """
        Encrypt and/or decrypt password or username based on the key
        :param string: username or password
        :param encrypt: False to decrypt an encrypted credential, True to encrypt user provided credential
        :return: encrypted or decrypted byte string
        """
        # get the key and create cipher
        key = self.key_location.read_bytes()
        fernet = Fernet(key)

        # strings should be encoded
        if isinstance(string, str):
            string = string.encode()

        if encrypt:
            return fernet.encrypt(string)

        return fernet.decrypt(string)

    # Note that ask_credentials and read_credentials both return encoded encrypted bytes.
    # they must all be decrypted and decoded before usage - some sort of uniformity
    def ask_credentials(self):
        """
        Output must be decrypted and decoded before connection to database
        :return: tuple of encrypted byte string: (username, password)
        """
        username = click.prompt("Username", type=str)
        password = click.prompt("Password", hide_input=True, type=str)
        return (
            self.encrypt_decrypt(username.encode(), encrypt=True),
            self.encrypt_decrypt(password.encode(), encrypt=True)
        )

    def read_credentials(self, flavor: str, name: str):
        """
        Output must be decrypted first then decoded
        :param flavor: one of the sql flavor/dialects used. oracle, mysql, etc.
        :param name: the name of a database available for one of the flavors
        :return: tuple of byte strings (username, password)
        """
        if not self.credential_location.exists():
            return None, None

        try:
            with self.credential_location.open('r') as f:
                credential_file = json.load(f)
        except (OSError, json.JSONDecodeError) as _:
            return None, None

        if flavor not in credential_file:
            return None, None

        if name not in credential_file[flavor]:
            return None, None

        username = credential_file[flavor][name].get('username')
        password = credential_file[flavor][name].get('password')

        return username.encode(), password.encode()

    def write_credentials(self, flavor: str, name: str, username: bytes, password: bytes, credential_dict=None):
        """
        Writes encrypted credentials to file
        :param flavor: one of the sql flavor/dialects used. oracle, mysql, etc.
        :param name: the name of a database available for one of the flavors
        :param username: username encrypted by key
        :param password: password encrypted by key
        :param credential_dict: optional dictionary with database credentials
        :return:
        """
        if not credential_dict:
            try:
                with self.credential_location.open('r') as f:
                    credential_dict = json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                raise e

        if flavor not in credential_dict:
            credential_dict[flavor] = {}

        credential_dict[flavor][name] = {
            'username': username.decode("utf-8"),
            'password': password.decode("utf-8")
        }

        try:
            with self.credential_location.open('w') as f:
                json.dump(credential_dict, f)
        except (OSError, TypeError) as e:
            raise e

        return None

    def add_new_database_info(self):
        """
        Add new database
        :return: None
        """
        try:
            with self.host_location.open('r') as f:
                host_file = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            raise e

        # database flavor
        flavor: str = click.prompt("Database flavor", type=click.Choice(FLAVORS_FOR_CONFIG))
        flavor = flavor.strip()
        if flavor not in host_file:
            host_file[flavor] = {}
        current_flavor_info = host_file.get(flavor, {})

        # database name
        name: str = click.prompt(
            "Name for database - for some flavors like mysql database name is used to create engine",
            type=str
        )
        name = name.strip()
        if name in current_flavor_info:
            confirmation = click.confirm(f"There exists a {name} database in your {flavor}. Do you wish to replace it?")
            if not confirmation:
                return None

        # additional_infos
        host = click.prompt("Host/Database Address or Snowflake Account", type=str)
        schema = click.prompt("Schema - Enter if none", default='', type=str)
        sid = click.prompt("SID - Enter if none", default='', type=str)
        service_name = click.prompt("Service Name - Enter if none", default='', type=str)
        port = click.prompt("Port Number - Enter if none", default=-1, type=int)

        host_dict = dict(name=name, host=host, schema=schema, port=port, service_name=service_name, sid=sid)

        # we don't want to store schema, service name or port if they are not required
        if not schema:
            _ = host_dict.pop('schema')

        if not service_name:
            _ = host_dict.pop('service_name')

        if not sid:
            _ = host_dict.pop('sid')

        if port == -1:
            _ = host_dict.pop('port')

        # set data
        host_file[flavor][name] = host_dict

        try:
            with self.host_location.open('w') as f:
                json.dump(host_file, f)
        except (OSError, TypeError) as er:
            raise er

        return None

    def remove_database(self):
        """
        remove a database and it's credentials
        :return: None
        """
        try:
            with self.host_location.open('r') as f:
                host_file = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            raise e

        # database flavor
        flavor: str = click.prompt("Database flavor", type=click.Choice(FLAVORS_FOR_CONFIG))
        flavor = flavor.strip()

        if flavor not in host_file:
            raise MissingFlavor(f"There are no databases with {flavor} flavor", None)

        # database name
        name: str = click.prompt(
            "Name for database - for some flavors like mysql database name is used to create engine",
            type=str
        )
        name = name.strip()

        if name not in host_file[flavor]:
            raise MissingDatabase(f"There are no {name} databases under the {flavor} flavor", None)

        _ = host_file[flavor].pop(name)

        try:
            with self.host_location.open('w') as f:
                json.dump(host_file, f)
        except (OSError, TypeError) as e:
            raise e

        self._remove_credential(flavor, name)

        return None

    # TODO - very similar to the method above. improve
    def _remove_credential(self, flavor: str, name: str):
        """
        removes credentials only
        :param flavor: one of the sql flavor/dialects used. oracle, mysql, etc.
        :param name: the name of a database available for one of the flavors
        :return:
        """
        try:
            with self.credential_location.open('r') as f:
                credential_file = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            raise e

        if flavor not in credential_file:
            return None

        if name not in credential_file[flavor]:
            return None

        _ = credential_file[flavor].pop(name)

        try:
            with self.credential_location.open('w') as f:
                json.dump(credential_file, f)
        except (OSError, TypeError) as e:
            raise e

        return None

    def reset_credentials(self):
        """
        reset credentials without testing if they are correct
        :return: None
        """
        try:
            with self.credential_location.open('r') as f:
                credential_dict = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            raise e

        warnings.warn("Database connection will not be tested with these credentials")

        # database flavor
        flavor: str = click.prompt("Database flavor", type=click.Choice(FLAVORS_FOR_CONFIG))
        flavor = flavor.strip()

        if flavor not in credential_dict:
            raise MissingFlavor(f"There are no databases with {flavor} flavor", None)

        # database name
        name: str = click.prompt(
            "Name for database - for some flavors like mysql database name is used to create engine",
            type=str
        )
        name = name.strip()

        if name not in credential_dict[flavor]:
            raise MissingDatabase(f"There are no {name} databases under the {flavor} flavor", None)

        username = credential_dict[flavor][name].get('username')
        username = self.encrypt_decrypt(username, encrypt=False)

        confirmation = click.confirm(f"Current username is {username}. Do you wish to keep it?")

        if not confirmation:
            username = click.prompt("Username", type=str)

        password = click.prompt("Password", hide_input=True, type=str)

        # encoded bytes
        username = self.encrypt_decrypt(username, encrypt=True)
        password = self.encrypt_decrypt(password, encrypt=True)

        # write
        self.write_credentials(flavor, name, username, password, credential_dict)

        return None

    def __str__(self):
        dsc = "dsdbmanager configurer with:\n"  # first line message

        host_path = "- host file at: " + str(self.host_location)  # second line
        host_path = host_path.rjust(len(host_path) + 4) + '\n'

        cred_path = "- credential file at: " + str(self.credential_location)  # third line
        cred_path = cred_path.rjust(len(cred_path) + 4) + '\n'

        key_path = "- key located at: " + str(self.key_location)  # fourth line
        key_path = key_path.rjust(len(key_path) + 4)

        return f"{dsc}{host_path}{cred_path}{key_path}"
