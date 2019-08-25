# dsdbmanager
Do you love SqlAlchemy? Do you usually have to connect to the same databases all the time for some quick data processing and/or exploration?

<ul>
<li>You might have passwords hard coded in scripts and have to make sure no one sees the code</li>
<li>You might be using environment variables to store credentials or host/ports etc</li>
</ul> 

When dealing with simple data processing (especially with different databases with relatively small tables), it does not always make sense
to have the same
```python
import sqlalchemy
import cx_Oracle

```

in the header of each script. There are many ways of dealing with these issues but this project tries to provide some example
for the new data scientist. This project is not meant to be used in production.

## Adding databases
It is recommended to set an envionment variable `DSDBMANAGER_CONFIG` pointing to a folder where the configuration files can be stored.
If the environment variable is not available, `pathlib.Path.home() / ".dsdbmanager"` is used by default.

Use entry point command `dsdbmanager add-database` directly in a command shell to add a database.

Adding databases directly in an interactive shell:

![add database](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/add_db.gif) 

This will add the database directly to the `host.json` file automatically created at first `import`.

![host json](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/host.png)

## Connecting

#### Manual Connection 
Once a database is added, it is easy to connect to it using one of the following modules (each corresponding to a sql flavor/dialect)
    
<ul>
<li>oracle</li>
<li>mssql</li>
<li>mysql</li>
<li>teradata</li>
</ul>

![mysql connect](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/manualconnection.png)

#### Connecting With Shortcut
In order to save encrypted credentials for reuse, the project comes with a shortcut for each flavor/dialect

![shortcut](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/using_shortcut.png)

This approach creates an object that has the name of each database as a method. The image above shows the database `dstest` as a method
that can be called with two parameters.

![connect](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/first_time.png) 

User is prompted for a username and password the first time. A connection is attempted and when successful, the credentials are stored in a json file and looks like this.

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
<li>Attribute 'm': provides the metadata for each table/view in the schema</li>
<li>Every table or view in the schema becomes an attribute that can be used to pull data into a dataframe</li>
<li>Attribute 'x': provides a function that uses a pandas dataframe to insert records into a table</li>
<li>Attribute 'u': provides a function that uses a pandas dataframe to update records in a table</li>
</ol>

![metadata](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/metadata.png)

![read data](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/read_table.png)

#### Context Manager
It is highly suggested to connect to the databases using a context manager approach in order to directly dispose of engines and discard properties.

![context manager](https://github.com/jojoduquartier/dsdbmanager/blob/master/source/imgs/as_context_manager.png)

In fact the context manager approach is way cleaner than the others

```python
from dsdbmanager import mysql

with mysql().dstest(connect_only=True, schema='myschema') as dbobject:
    # do something
    pass

```