import json
from .dbobject import DsDbManager
from .configuring import ConfigFilesManager

configurer = ConfigFilesManager()

# first initialize empty files
# if there are no host files, create an empty json file #
if not configurer.host_location.exists():
    try:
        configurer.host_location.touch(exist_ok=True)
        with open(configurer.host_location, "w") as f:
            json.dump({}, f)
    except OSError as e:
        raise Exception("Could not write at host file location", e)

# if there are credential files create an empty json file #
if not configurer.credential_location.exists():
    configurer.credential_location.touch(exist_ok=True)
    try:
        with open(configurer.credential_location, 'w') as f:
            json.dump({}, f)
    except OSError as e:
        raise Exception("Could not write at credential file location", e)

# if there are no keys, create one and store it at key location #
if not configurer.key_location.exists():
    configurer.key_location.touch(exist_ok=True)
    configurer.key_location.write_bytes(configurer.generate_key())

# functions for users to use
add_database = configurer.add_new_database_info
remove_database = configurer.remove_database
reset_credentials = configurer.reset_credentials


# easy access for databases
def oracle():
    return DsDbManager('oracle')


def teradata():
    return DsDbManager('teradata')


def mysql():
    return DsDbManager('mysql')


def mssql():
    return DsDbManager('mssql')
