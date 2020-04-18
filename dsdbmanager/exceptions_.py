"""
Custom Exceptions
"""


class BaseException_(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class EmptyHostFile(BaseException_):
    """
    If the host file is empty this exception will be raised
    """

    def __init__(self, message, errors=None):
        super().__init__(
            f"{message}. Consider adding a database using dsdbmanager.add_database()", errors)


class MissingFlavor(BaseException_):
    """
    If a given flavor is not in the host file then this should be raised
    """

    def __init__(self, message, errors=None):
        super().__init__(
            f"{message}. Consider adding a database using dsdbmanager.add_database()", errors)


class MissingDatabase(BaseException_):
    """
    If the database has not been added to the flavor then it raise this error
    """

    def __init__(self, message, errors=None):
        super().__init__(
            f"{message}. Consider adding a database using dsdbmanager.add_database()", errors)


class NoSuchColumn(BaseException_):
    pass


class BadArgumentType(BaseException_):
    pass


class OperationalError(BaseException_):
    pass


class NotImplementedFlavor(BaseException_):
    pass


class MissingPackage(BaseException_):
    pass


class InvalidSubset(BaseException_):
    pass
