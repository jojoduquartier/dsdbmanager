# Changelog

## [Unreleased]
### Added
- `from_engine` shortcut added to `dsdbmanager`. This will create `dsdbobject.DbMiddleware` objects out of sqlalchemy engines.
This is mainly for sqlite and other flavor/dialects that are yet to be implemented here. With the sqlite engines, testing will be easy.

### Changed
- `engine` property is now `sqlalchemy_engine` for `dsdbobject.DbMiddleware` class.
- pre-configured `schema` is now used when available. User does not have to specify the schema if they had it added