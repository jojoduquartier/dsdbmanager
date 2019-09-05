"""
Custom Exceptions

1) No {Flavor} databases available
2) No Such Database for {flavor}
3) No Such Column
4) FailedUpdated
5) FailedInsert
6) BadArgumentType - this goes well with the exception where a sqlalchemy table is not provided, inherit from ValueError
7) NonImplementedFlavor - for when they try to use a different dialect that is not available
8) EmptyHostFile - probably only usable in dbobject.py since the configurer will handle its own errors
"""
