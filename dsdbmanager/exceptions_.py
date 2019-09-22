"""
Custom Exceptions
"""


class BaseException(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class EmptyHostFile(BaseException):
    """
    If the host file is empty this exception will be raised
    """

    def __init__(self, message, errors=None):
        super().__init__(f"{message}. Consider adding a database using dsdbmanager.add_database()", errors)


class MissingFlavor(BaseException):
    """
    If a given flavor is not in the host file then this should be raised
    """

    def __init__(self, message, errors=None):
        super().__init__(f"{message}. Consider adding a database using dsdbmanager.add_database()", errors)


class MissingDatabase(BaseException):
    """
    If the database has not been added to the flavor then it raise this error
    """

    def __init__(self, message, errors=None):
        super().__init__(f"{message}. Consider adding a database using dsdbmanager.add_database()", errors)


class NoSuchColumn(BaseException):
    pass


class BadArgumentType(BaseException):
    pass


class OperationalError(BaseException):
    pass


class NotImplementedFlavor(BaseException):
    pass


class MissingPackage(BaseException):
    pass
