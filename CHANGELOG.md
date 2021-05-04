# Changelog

## [Unreleased]
### Added
- `from_engine` shortcut added to `dsdbmanager`. This will create `dsdbobject.DbMiddleware` objects out of sqlalchemy engines.
This is mainly for sqlite and other flavor/dialects that are yet to be implemented here. With the sqlite engines, testing will be easy.

### Changed
- `engine` property is now `sqlalchemy_engine` for `dsdbobject.DbMiddleware` class.
- pre-configured `schema` is now used when available. User does not have to specify the schema if they had it added

## [Version 1.0.0]
First release deployed to PyPI

## [Version 1.0.1]
There was a bug with the way credentials were stored. This caused the whole credential branch
for a dialect to be replaced. It has been fixed

## [Version 1.0.2]
The snowflake dialect has been added 

## [Version 1.0.3]
1.0.2 build didn't have the wheel files and the requirements.txt was not included
the manifest file has been added and a wheel file created

## [Version 1.0.4]
- sqlalchemy_engine is read only
- host, credential and key attibutes of configurer are read only
- user can now create subsets. In the root folder where host is created, you will have a `subsets` folder. the goal is to be able to create different subsets for each project. Say I have 10 databases, 5 oracle and 5 mysql but I only need 2 each for a specific project. You can create a subset with a totally different encryption key that you can use on the server you want the project to run.

## [Version 1.0.5]
- no breaking changes
- snowflake connection is rather different so changes were made so that user can enter all snowflake specific connection parameters
- If users want to use the snowflake connection object, they can use a `raw_connection` argument in `dsdbmanager.snowflake_.Snowflake.create_engine`

## [Version 1.0.6]
- no breaking changes
- mysql and mssql doesn't always use database names in connection string. I encountered cases like these recently so I added a boolean field
when adding new databases. The field `use_dbname_to_connect` should be `True` if user wants database name to be used in database connection and `False` otherwise.
This change should only impact mysql and mssql connections.