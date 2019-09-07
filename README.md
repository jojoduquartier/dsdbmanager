# dsdbmanager
Data Science DataBase Manager

Do you love SqlAlchemy? Do you usually have to connect to the same databases all the time for some quick data processing and/or exploration?

<ul>
<li>You might have database address and/or credentials hardcoded in a script</li>
<li>You might be using environment variables to store credentials or host/ports etc</li>
</ul> 

When dealing with simple data processing (especially with different databases with relatively small tables), it does not always make sense
to have the something like
```python
import sqlalchemy
import cx_Oracle

```

in the header of each script. There are many ways of dealing with these issues but this project tries to provide an example
for the new data scientist. This project is not meant to be used in production; the goal is to speed up exploration by eliminating 
some frequent database connection activities.

* The examples below are based on a locally hosted [MySql](https://www.mysql.com/products/workbench/) database

Consider the following database **dstest** with tables **category** and **user**. The database host, schema and/or port can be
stored in a json file that the package uses to quickly create sqlalchemy engines. 

![mysql workbench](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/workbench.png)

## Adding databases
After (or before) installation of the package, set an envionment variable `DSDBMANAGER_CONFIG` pointing to a folder where the configuration files can be stored.
If the environment variable is not available, `pathlib.Path.home() / ".dsdbmanager"` is used by default.

Use entry point command `dsdbmanager add-database` directly in a command shell to add a database.

Or any python interactive interactive shell:

![add database](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/add_db.gif) 

This will add the database directly to the `.host.json` file automatically created at first `import`.

![host json](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/host.png)

## Connecting

#### Manual Connection 
Once a database is added, it is easy to connect to it using one of the following modules (each corresponding to a sql flavor/dialect) 
to establish a connection. This approach means that the user only needs to provide the username and password to create the engines.
    
<ul>
<li>oracle_</li>
<li>mssql_</li>
<li>mysql_</li>
<li>teradata_</li>
</ul>

![mysql connect](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/manualconnection.png)

#### Connecting With Shortcut
In order to save encrypted credentials for reuse, the project comes with a shortcut for each flavor/dialect

![shortcut](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/using_shortcut.png)

This approach creates an object that has the name of each database as a method. The image above shows the only mysql database `dstest` as a property.
This property is actually a function which must be called to establish connection.

![connect](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/first_time.png) 

Because the connection is only made when the function is called, all the databases available are just properties until the user needs them.
When a connection is attempted for the first time, the user is prompted for the username and password. The credentials are used to create the
sqlalchemy engine which is used to test for successful connection. If the connection fails, the credentials are discarded and an error is raised. If
the connection succeeds, the credentials are encrypted and stored in a file like this:

```json
{
    "mysql": {
        "dstest": {
            "username": "gAAAAABdYe_Le1rX3W5y23GlLf0dtrVVOWJhaPGVk2_CbIfpcqb_0dzu5_MFJpgTRuXF7EKk3UcLvCI5HyjP6b5daZQoMJRM2g==",
            "password": "gAAAAABdYe_LtuwnY95B0nhsSKQbe8DEuvhbjO2Y9zo-PwC_UqsmQ1whRsGyTlZGc3RRyWc3yde6cGozxPJjcjZv77itSuyKVg=="
        }
    }
}
```

The credentials are retrieved every other time so that the user never has to pass the host or password info when needed.

#### Connect_Only = False
What happens when `connect_only = False`? The object created has not only an engine but:

<ol>
<li>Attribute '_metadata': provides the metadata for each table/view in the schema

![metadata](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/metadata.png)
</li>

<li>Every table or view in the schema becomes an attribute that can be used to pull data into a dataframe

![read data](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/read_table.png)
</li>

<li>Attribute '_insert': provides a function that uses a pandas dataframe to insert records into a table

![read data](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/insert.png)

The picture above shows that `2` records were inserted in the `category` table of `dstest` and we can see it here

![read data](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/inserted.png)
</li>
<li>Attribute '_update': provides a function that uses a pandas dataframe to update records in a table</li>
</ol>

#### Context Manager
It is possible to connect to the databases with a context manager approach and this is highly suggested. Sqlachemy engines
are disposed and properties pointing to tables etc. are all cleared

![context manager](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/as_context_manager.png)

The context manager approach is way cleaner anyways.
```python
from dsdbmanager import mysql

with mysql().dstest(connect_only=True, schema=None) as dbobject:
    # anything with the engine goes here
    pass
```