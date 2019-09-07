import unittest
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc as exc
from dsdbmanager.dbobject import (
    util_function,
    insert_into_table,
    update_on_table,
    table_middleware,
    db_middleware,
    DbMiddleware,
    TableMeta,
    TableInsert,
    TableUpdate
)


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
        pass


if __name__ == '__main__':
    unittest.main()
