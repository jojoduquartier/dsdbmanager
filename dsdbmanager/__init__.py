import json
from .dbobject import DsDbManager, DbMiddleware
from .configuring import ConfigFilesManager

__version__ = '1.0.1'
configurer = ConfigFilesManager()

# first initialize empty files
# if there are no host files, create an empty json file #
if not configurer.host_location.exists():
    try:
        configurer.host_location.touch(exist_ok=True)
        with configurer.host_location.open('w') as f:
            json.dump({}, f)
    except OSError as e:
        raise Exception("Could not write at host file location", e)

# if there are credential files create an empty json file #
if not configurer.credential_location.exists():
    configurer.credential_location.touch(exist_ok=True)
    try:
        with configurer.credential_location.open('w') as f:
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


def from_engine(engine, schema: str = None):
    """
    Main objective is to use this to create DbMiddleware objects on sqlite engines for quick testing purposes

    :param engine:
    :param schema:
    :return:

    >>> import pandas as pd
    >>> from sqlalchemy import create_engine

    # create engine
    >>> engine = create_engine('sqlite://')

    # insert data
    >>> df = pd.DataFrame({'a':[1,2,3], 'b':[1,2,3]})
    >>> df.to_sql('test', engine, index=False)

    # create a db object
    >>> db = from_engine(engine)

    >>> df1: pd.DataFrame = db.test()
    >>> df1.equals(df)
    True
    >>>

    """
    return DbMiddleware(engine, False, schema)
