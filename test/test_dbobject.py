import json
import unittest
import pathlib
import tempfile
import contextlib
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc as exc
from dsdbmanager.dbobject import (
    util_function,
    insert_into_table,
    update_on_table,
    table_middleware,
    DbMiddleware,
    DsDbManager,
    TableMeta,
    TableInsert,
    TableUpdate
)
from dsdbmanager.exceptions_ import (
    BadArgumentType,
    NoSuchColumn,
    NotImplementedFlavor,
    EmptyHostFile,
    MissingFlavor
)
from dsdbmanager.configuring import ConfigFilesManager


class TestDbObject(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        metadata = sa.MetaData()

        cls.currency_table = sa.Table(
            'currency',

            metadata,

            sa.Column(
                'denomination',
                sa.String(100),
                primary_key=True
            ),

            sa.Column(
                'abbreviation',
                sa.String(20),
                primary_key=True
            ),

            sa.Column(
                'countries',
                sa.String(500),
                primary_key=True
            )
        )

        cls.country_table = sa.Table(
            'country',

            metadata,

            sa.Column(
                'country',
                sa.String(20),
                primary_key=True
            ),

            sa.Column(
                'continent',
                sa.String(20)
            )
        )

        cls.engine_function = lambda _: sa.create_engine("sqlite:///")

        cls.host = {
            'oracle': {
                'mydatabase': {
                    'name': 'mydatabase',
                    'host': 'localhost',
                    'sid': 'pyt'
                },
                'newdatabase': {
                    'name': 'newdatabase',
                    'host': 'localhost',
                    'sid': 'pyt',
                    'schema': 'schemo'
                }
            }
        }

    @classmethod
    @contextlib.contextmanager
    def generate_path(cls, suffix=None) -> pathlib.Path:
        try:
            temp = tempfile.NamedTemporaryFile(suffix=suffix) if suffix else tempfile.NamedTemporaryFile()
            yield pathlib.Path(temp.name)
        except OSError as _:
            temp = None
        finally:
            if temp is not None:
                temp.close()

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.engine: sa.engine.Engine = self.engine_function()
        self.currency_table.create(self.engine)
        self.country_table.create(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_util_function(self):
        """
        1) check that the tables are in the engine
        2) check that the output of the function are sqlalchemy tables
        3) check that the wrong table name yields an error
        :return:
        """
        self.assertIn(self.currency_table.name, self.engine.table_names())
        self.assertIn(self.country_table.name, self.engine.table_names())

        self.assertIsInstance(
            util_function(
                table_name=self.country_table.name,
                engine=self.engine,
                schema=None
            ),
            sa.Table
        )

        with self.assertRaises(exc.NoSuchTableError):
            util_function(
                table_name='not_there',
                engine=self.engine,
                schema=None
            )

    def test_insert_into_table(self):
        """

        :return:
        """
        df = pd.DataFrame(
            {
                'denomination': ['United States Dollar', 'West African Franc CFA'],
                'abbreviation': ['USD', 'Franc CFA'],
                'countries': ['U.S.A', "Benin, Togo, Burkina Faso, Guinea-Bissau, Cote D'Ivoire, Mali, Niger, Senegal"]
            }
        )

        inserted = insert_into_table(
            df=df,
            table_name=self.currency_table.name,
            engine=self.engine,
            schema=None
        )

        read_df = pd.read_sql(
            self.currency_table.name,
            self.engine
        )

        self.assertEqual(inserted, len(df))
        self.assertEqual(inserted, len(read_df))

        for column in (column.name for column in self.currency_table.columns):
            with self.subTest(column=column):
                self.assertIn(column, read_df.columns)

    def test_update_on_table(self):
        """

        :return:
        """

        # records to insert
        country = [
            {
                'country': 'U.S.A',
                'continent': 'North America'
            },
            {
                'country': 'Benin',
                'continent': 'Africa'
            },
            {
                'country': 'Japan',
                'continent': 'East Asia'
            }
        ]

        # execute insert
        insert = self.engine.execute(
            self.country_table.insert(),
            country
        )
        insert.close()

        # update using tuples as keys
        us_update = pd.DataFrame(
            {
                'country': ['U.S.A'],
                'continent': ['America']
            }
        )
        updated = update_on_table(
            df=us_update,
            keys=('country',),
            values=('continent',),
            table_name=self.country_table.name,
            engine=self.engine,
            schema=None
        )
        us_current_value = self.engine.execute(
            sa.select([self.country_table.c['continent']]).where(
                self.country_table.c['country'] == 'U.S.A'
            )
        ).fetchall()

        self.assertEqual(updated, 1)
        self.assertEqual(updated, len(us_current_value))
        self.assertEqual(us_current_value[0][0], us_update.loc[0, 'continent'])

        # update using dictionary as keys
        japan_update = pd.DataFrame(
            {
                'the country': ['Japan'],
                'the continent': ['Asia']
            }
        )
        updated = update_on_table(
            df=japan_update,
            keys={'country': 'the country'},
            values={'continent': 'the continent'},
            table_name=self.country_table.name,
            engine=self.engine,
            schema=None
        )
        japan_current_value = self.engine.execute(
            sa.select([self.country_table.c['continent']]).where(
                self.country_table.c['country'] == 'Japan'
            )
        ).fetchall()

        self.assertEqual(updated, 1)
        self.assertEqual(updated, len(japan_current_value))
        self.assertEqual(japan_current_value[0][0], japan_update.loc[0, 'the continent'])

        # errors on providing the wrong type of arguments for key etc
        with self.assertRaises(BadArgumentType):
            update_on_table(
                df=japan_update,
                keys='country',
                values='continent',
                table_name=self.country_table.name,
                engine=self.engine,
                schema=None
            )

    def test_table_middleware(self):
        """

        :return:
        """
        read_from_currency_table = table_middleware(
            engine=self.engine,
            table=self.currency_table.name
        )
        currencies = [
            {
                'denomination': 'US Dollar',
                'abbreviation': 'USD',
                'countries': 'U.S.A'
            },
            {
                'denomination': 'Euro',
                'abbreviation': 'EUR',
                'countries': 'A bunch'
            }
        ]

        insert = self.engine.execute(
            self.currency_table.insert(),
            currencies
        )
        insert.close()

        self.assertEqual(read_from_currency_table().shape, (len(currencies), 3))
        self.assertEqual(read_from_currency_table(rows=1).shape, (1, 3))
        self.assertEqual(read_from_currency_table(rows=1, columns=('abbreviation', 'countries')).shape, (1, 2))
        self.assertEqual(read_from_currency_table(abbreviation='USD').shape, (1, 3))
        self.assertTrue(read_from_currency_table(abbreviation='FCFA').empty)

        with self.assertWarnsRegex(UserWarning, r"Columns \[made_up, not there\] are not in table currency"):
            read_with_warning = read_from_currency_table(columns=('abbreviation', 'countries', 'not there', 'made_up'))
            self.assertEqual(read_with_warning.shape, (len(currencies), 2))

        # query bad columns
        with self.assertRaises(NoSuchColumn):
            _ = read_from_currency_table(columns=('madeup', 'made up'))

    def test_dbmiddleware(self):
        """

        :return:
        """

        with DbMiddleware(self.engine, connect_only=False, schema=None) as dbm:
            for attr in (
                    self.currency_table.name,
                    self.country_table.name,
                    'sqlalchemy_engine',
                    '_metadata',
                    '_insert',
                    '_update'
            ):
                with self.subTest(attribute=attr):
                    self.assertTrue(hasattr(dbm, attr))

            self.assertIsInstance(dbm._metadata, TableMeta)
            self.assertIsInstance(dbm._insert, TableInsert)
            self.assertIsInstance(dbm._update, TableUpdate)

    def test_dsdbmanager(self):
        with self.assertRaises(NotImplementedFlavor):
            _ = DsDbManager('somemadeupflavor')

        with self.generate_path(suffix='.json') as hp, self.generate_path(
                suffix='.json') as cp, self.generate_path() as kp:
            c = ConfigFilesManager(
                hp,
                cp,
                kp
            )

            # test empty host file
            with hp.open('w') as f:
                json.dump({}, f)

            with self.assertRaises(EmptyHostFile):
                _ = DsDbManager('oracle', c)

            # test with actual host data
            with hp.open('w') as f:
                json.dump(self.host, f)

            dbobject = DsDbManager('oracle', c)

            # test for missing flavor
            with self.assertRaises(MissingFlavor):
                _ = DsDbManager('teradata', c)

            # some properties must be there by default
            for attribute in [
                '_flavor',
                '_config_file_manager',
                '_host_dict',
                '_available_databases',
                '_schemas_per_databases',
                '_connection_object_creator',
            ]:
                with self.subTest(attribute=attribute):
                    self.assertTrue(hasattr(dbobject, attribute))

            # properties based on host file
            for host_database in self.host.get('oracle'):
                with self.subTest(host_database=host_database):
                    self.assertTrue(hasattr(dbobject, host_database))

            # the host_dict should be the same dictionary as host
            self.assertEqual(self.host, dbobject._host_dict)

            # two available databases and two schemas
            self.assertEqual(len(dbobject._available_databases), 2)
            self.assertEqual(len(dbobject._schemas_per_databases), 2)
            self.assertEqual(dbobject._available_databases, ['mydatabase', 'newdatabase'])
            self.assertEqual(dbobject._schemas_per_databases, [None, 'schemo'])


# TODO - test TableMeta, TableInsert, TableUpdate: check for keyerror etc


if __name__ == '__main__':
    unittest.main()
