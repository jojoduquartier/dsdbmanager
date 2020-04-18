Introduction
=============

Objective
^^^^^^^^^^
The main goal of this project is to allow anyone working with relational databases to:

    1. Create an Oracle, Mysql ... engine by only providing a username and a password.
    2. Store and Organize database information such as the host, port and schema etc.

That really is it. All the additional work to speed up some data science development is extra and will be explained later.

Setup
*******

First, make sure to have :code:`DSDBMANAGER_CONFIG` set as an environment variable pointing to a secure folder. This is where
the database information will be stored. If the environment variable is not available, a default is directly set the first time
the package is imported. That default corresponds to :code:`pathlib.Path.home() / ".dsdbmanager"`.

To install the package simply do::

    pip install dsdbmanager


Once installed, the package provides a set of shell commands to add databases or remove them.

Add a Database
*****************

In a shell command, type :code:`dsdbmanager add-database`
In a python interactive shell::

    python
    from dsbmanager import add_database
    add_database()

Either approach will ask you a series of questions that will lead to the creation of a :code:`.hosts.json` file that is well
structured like so::

    {
      "mysql": {
        "database1": {
          "name": "database1",
          "host": "localhost",
          "port": 3306
        },
        "database 2": {
          "name": "database2",
          "host": "my.host.com",
          "port": 3306
        }
      },
      "oracle": {
        "database1": {
          "name": "database1",
          "host": "oracle.myhost.com",
          "port": 1521,
          "schema": "myschema",
          "sid": "orcl"
        }
      }
    }

Note: Keep in mind that in order to use mysql, oracle or any other dialects, the corresponding sqlalchemy extensions should be installed
as well as any additional OS requirements. For example: Oracle clients and required software must be available in order to get
:code:`cx_Oracle` and :code:`sqlalchemy` to generate a proper engine.

Creating Engines
*****************

Armed with a structure like the one above, it is easy to distinguish oracle databases from mysql databases etc. This means
that we can easily determine the proper channels to create an engine. Sure it takes little to no effort to copy the engine
configuration line from SqlAlchemy but when working with different Jupyter Notebooks, it's easier to go the following route::

    import dsdbmanager

    mysql_database1 = dsdbmanager.mysql_.Mysql('database1').create_engine(username, password)
    oracle_database1 = dsdbmanager.oracle_.Oracle('database1').create_engine(username, password)

You can also pass additional Sqlalchemy arguments to the :code:`create_engine` call.

That really is the main objective of this project.

But to build on this concept of structuring the databases and their information, there are some extra perks

Extras
^^^^^^^

If I can organize my databases this way, shouldn't I be able to have a similar file for the credentials?

Yes! Well it is definitely possible but credentials are very sensitive information. So before storing them, the package offers a way
to encrypt them. This means that a credential file similar in structure to the :code:`.hosts.json` file with totally encrypted
usernames and passwords. The key to decrypt and encrypt is generated the very first time the package is imported/used.

The good thing about the credentials being stored aside is that they will never show in your jupyter notebooks or scripts. To achieve this,
you can do::

    import dsdbmanager

    mysql_database1 = dsdbmanager.mysql().database1(connect_only=False)
    mysql_database2 = dsdbmanager.mysql()['databases'](connect_only=False)
    oracle_database1 = dsdbmanager.oracle().database1(connect_only=False)
    oracle_database1_with_newschema = dsdbmanager.oracle().database1(connect_only=False, schema='newschema')

    # to access the engines, use the sqlalchemy_engine property. For example
    engine = mysql_database1.sqlalchemy_engine

The first time a connection is being attempted, you will be asked for credentials. Those credentials will then be encrypted and stored
if the connection is successful.

Why are there a :code:`connect_only=True` and additional :code:`schema` arguments available when connecting to the databases.

The approach above is wrapping the :code:`sqlalchemy engine` in a :code:`dsdbmanager.dboject.DbMiddleware` object. Please read the docs on this object.
It has a property :code:`sqlalchemy_engine` that provides the :code:`sqlalchemy engine` but it also has **all the tables and views in the schema of the database as properties**.
These properties are actually just functions so you are not reading anything from the database unless you call those functions. This is why there is an option to
specify a different schema than the one specified when adding the database (because it would not make sense to have a different json entry for each schema on a database).

Something very important to note: Those functions that when called bring you data from the database, they automatically cache the data. So if somehow your function took
a minute to bring the data you need, the next time you call the function, it will take no time at all. That also means that any changes on the database would not be reflected
in your new function calls. That is one of the reasons why the :code:`dsdbmanager.dboject.DbMiddleware` can be used as a context manager.

Well that's cool but perhaps you do not want to store your credentials. Maybe you want to pass your username and password to create the engine and then
make use of the :code:`dsdbmanager.dboject.DbMiddleware` class. There is a :code:`from_engine` function for that. For example::

    import dsdbmanager

    mysql_database1_engine = dsdbmanager.mysql_.Mysql('database1').create_engine(username, password)
    mysql_database1 = dsdbmanager.from_engine(mysql_database1_engine, schema="some_schema")

This effectively simplifies some simple queries like :code:`select * from table` or :code:`select column1, column2 from table limit 10` for example. That is because
the functions mentioned above take arguments :code:`rows` and :code:`columns`. Look at the source code for :code:`dsdbmanager.dboject.table_middleware`

It is also possible to do::

    import dsdbmanager
    mysql_database1 = dsdbmanager.mysql().database1(connect_only=False)
    mysl_database1.table_1(rows=10, columns=('column 1', 'column_2'), column_3 = value_1, **{'column 4': value_2}, column_5 = (value_3, value_4))

The last command is equivalent to :code:`select [column 1], column_2 from table_1 where column_3 = value_1 and [column 4] = value_2 and column_5 in (value_3, value_4)`.

    1. You will have to use a dictionary to handle columns with spaces or begin with numbers for example.
    2. When you provide a tuple as a value, you are indicating a :code:`key in values` type filtering.
    3. If your table names have spaces or begin with numbers for example, you couldn't use the :code:`.` notation so you can do :code:`mysl_database1[table 2]` for example.

Creating Subsets By Project
*****************************
Say you are working on many projects locally and as a result you have many hosts/credentials saved. Say Project_x only uses a subset
and now the project must be moved to a server. It would not make sense to move the whole set of credentials to that server and the
key used locally should not be shared. It is possible to do::

    from dsdbmanager import create_subset
    create_subset({'oracle': 'db1', 'mysql': {'db1', 'db2'}, 'mssql': 'all'}, 'project_x')

The benefits of this is that it creates folders with new key and re-encrypted credentials. The folder can easily be moved wherever 
user desires. User can then move the key out of the folder and point the `DSDBMANAGER_KEY` variable to the path. Using `all` as 
above means that user wants to include all databases for a given dialect. This means that it is easy to re-encrypt all your 
credentials::

    create_subset({'oracle': 'all', 'mysql': 'all', 'mssql': 'all'}, 'monthly_re_encryption')



Footnote - Disclaimer
^^^^^^^^^^^^^^^^^^^^^^

    1. Saving these type of info is best on a drive that is not locally and well protected by firewall rules!
    2. It is also possible to separate the encryption key from the credentials with the `DSDBMANAGER_KEY` environment variable which should point to a path!
