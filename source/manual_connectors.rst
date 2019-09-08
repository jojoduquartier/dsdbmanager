Manual Connecting without Storing Credentials
===============================================

Connect to Oracle Databases
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once databases are added, this class can be used to instantiate an object for the proper :code:`database name`. The instance
will have a :code:`create_engine` method that should be used with just :code:`username` and :code:`password`.

.. autoclass:: dsdbmanager.oracle_.Oracle
   :members:

Connect to Mysql Databases
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once databases are added, this class can be used to instantiate an object for the proper :code:`database name`. The instance
will have a :code:`create_engine` method that should be used with just :code:`username` and :code:`password`.

.. autoclass:: dsdbmanager.mysql_.Mysql
   :members:

Connect to Mssql Databases
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once databases are added, this class can be used to instantiate an object for the proper :code:`database name`. The instance
will have a :code:`create_engine` method that should be used with just :code:`username` and :code:`password`.

.. autoclass:: dsdbmanager.mssql_.Mssql
   :members:

Connect to Teradata Databases
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once databases are added, this class can be used to instantiate an object for the proper :code:`database name`. The instance
will have a :code:`create_engine` method that should be used with just :code:`username` and :code:`password`.

.. autoclass:: dsdbmanager.teradata_.Teradata
   :members:
