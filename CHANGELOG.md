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