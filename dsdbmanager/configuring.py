import json
import click
import typing
import pathlib
import warnings
from cryptography.fernet import Fernet
from .constants import HOST_PATH, CREDENTIAL_PATH, KEY_PATH, FLAVORS_FOR_CONFIG


class ConfigFilesManager(object):
    def __init__(self):
        self.host_location: pathlib.Path = HOST_PATH
        self.credential_location: pathlib.Path = CREDENTIAL_PATH
        self.key_location: pathlib.Path = KEY_PATH

    @classmethod
    def generate_key(cls):
        return Fernet.generate_key()

    def get_hosts(self):
        try:
            with open(self.host_location, 'r') as f:
                hosts = json.load(f)

        except Exception as e:
            hosts = {}

        return hosts

    def encrypt_decrypt(self, string: typing.Union[str, bytes], encrypt: bool) -> typing.Union[str, bytes]:
        """
        Encrypt and/or decrypt password or username based on the key
        Args:
            string:
            encrypt:

        Returns: encoded string

        """
        # get the key and create cipher
        key = self.key_location.read_bytes()
        fernet = Fernet(key)

        if encrypt:
            return fernet.encrypt(string)

        return fernet.decrypt(string)

    # Note that ask_credentials and read_credentials both return encoded encrypted bytes.
    # they must all be decrypted and decoded before usage - some sort of uniformity
    def ask_credentials(self):
        """
        Output must be decrypted and decoded before connection to database
        Returns:

        """
        username = click.prompt("Username", type=str)
        password = click.prompt("Password", hide_input=True, type=str)
        return self.encrypt_decrypt(username, encrypt=True), self.encrypt_decrypt(password, encrypt=True)

    def read_credentials(self, flavor: str, name: str):
        """
        Output must be decrypted first then decoded
        Args:
            flavor:
            name:

        Returns:

        """
        if not self.credential_location.exists():
            return None

        try:
            with open(self.credential_location) as f:
                credential_file = json.load(f)
        except OSError as _:
            return None

        if flavor not in credential_file:
            return None

        if name not in credential_file[flavor]:
            return None

        username = credential_file[flavor][name].get('username')
        password = credential_file[flavor][name].get('password')

        return username.encode(), password.encode()

    def write_credentials(self, flavor: str, name: str, username: bytes, password: bytes, credential_dict=None):
        """
        Writes encrypted credentials to file
        Args:
            flavor:
            name:
            username:
            password:
            credential_dict:

        Returns:

        """
        if not credential_dict:
            try:
                with open(self.credential_location) as f:
                    credential_dict = json.load(f)
            except OSError as e:
                raise e

        if flavor not in credential_dict:
            credential_dict[flavor] = {}

        if name not in credential_dict[flavor]:
            credential_dict[flavor] = {
                name: {
                    'username': username.decode("utf-8"),
                    'password': password.decode("utf-8")
                }
            }
        else:
            credential_dict[flavor][name] = {
                'username': username.decode("utf-8"),
                'password': password.decode("utf-8")
            }

        try:
            with open(self.credential_location, 'w') as f:
                json.dump(credential_dict, f)
        except OSError as e:
            raise e

        return None

    def add_new_database_info(self):
        try:
            with open(self.host_location) as f:
                host_file = json.load(f)
        except OSError as e:
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
        host = click.prompt("Host/Database Address", type=str)
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
            with open(self.host_location, 'w') as f:
                json.dump(host_file, f)
        except OSError as er:
            raise er

        return None

    def remove_database(self):
        try:
            with open(self.host_location) as f:
                host_file = json.load(f)
        except OSError as e:
            raise e

        # database flavor
        flavor: str = click.prompt("Database flavor", type=click.Choice(FLAVORS_FOR_CONFIG))
        flavor = flavor.strip()

        if flavor not in host_file:
            raise Exception(f"There are no databases with {flavor} flavor")

        # database name
        name: str = click.prompt(
            "Name for database - for some flavors like mysql database name is used to create engine",
            type=str
        )
        name = name.strip()

        if name not in host_file[flavor]:
            raise Exception(f"There are no {name} databases under the {flavor} flavor")

        _ = host_file[flavor].pop(name)

        try:
            with open(self.host_location, 'w') as f:
                json.dump(host_file, f)
        except OSError as e:
            raise e

        self.remove_credential(flavor, name)

        return None

    # TODO - very similar to the method above. improve
    def remove_credential(self, flavor: str, name: str):
        try:
            with open(self.credential_location) as f:
                credential_file = json.load(f)
        except OSError as e:
            raise e

        if flavor not in credential_file:
            return None

        if name not in credential_file[flavor]:
            return None

        _ = credential_file[flavor].pop(name)

        try:
            with open(self.credential_location, 'w') as f:
                json.dump(credential_file, f)
        except OSError as e:
            raise e

        return None

    def reset_credentials(self):
        try:
            with open(self.credential_location) as f:
                credential_dict = json.load(f)
        except OSError as e:
            raise e

        warnings.warn("Database connection will not be tested with these credentials")

        # database flavor
        flavor: str = click.prompt("Database flavor", type=click.Choice(FLAVORS_FOR_CONFIG))
        flavor = flavor.strip()

        if flavor not in credential_dict:
            raise Exception(f"There are no databases with {flavor} flavor")

        # database name
        name: str = click.prompt(
            "Name for database - for some flavors like mysql database name is used to create engine",
            type=str
        )
        name = name.strip()

        if name not in credential_dict[flavor]:
            raise Exception(f"There are no {name} databases under the {flavor} flavor")

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
