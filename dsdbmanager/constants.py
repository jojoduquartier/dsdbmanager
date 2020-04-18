import os
import pathlib

# there should be a folder with the host file and the credentials path
try:
    config_folder = pathlib.Path(os.environ["DSDBMANAGER_CONFIG"])
except KeyError:
    config_folder = pathlib.Path.home() / ".dsdbmanager"

# create folder
try:
    config_folder.mkdir(parents=True)
except OSError:
    # TODO what makes sense here?
    pass

HOST_PATH = config_folder / ".hosts.json"
CREDENTIAL_PATH = config_folder / ".config.json"

# the cryptography key can be in the same folder or at a separate location
try:
    KEY_PATH = os.environ["DSDBMANAGER_KEY"]
except KeyError:
    KEY_PATH = config_folder / ".configkey"

# database flavors
FLAVORS_FOR_CONFIG = ('oracle', 'mysql', 'mssql', 'teradata', 'snowflake')

CACHE_SIZE = 64
CHUNK_SIZE = 30000
