import typing
import unittest
import numpy as np
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.sql.elements as sqlelements
from sqlalchemy.ext.declarative import declarative_base
from dsdbmanager.utils import d_frame, inspect_table, filter_maker


class TesUtil(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Base = declarative_base()

        class Students(Base):
            __tablename__ = 'students'

            first_name = sa.Column(sa.String(20), primary_key=True)
            last_name = sa.Column(sa.String(20), primary_key=True)
            middle_name = sa.Column(sa.String(20), nullable=True)
            gender = sa.Column(sa.String(1), nullable=True)
            age = sa.Column(sa.Integer, nullable=False)

        @d_frame
        def array_and_columns(_):
            return np.zeros((10, 2)), ('column1', 'column2',)

        @d_frame(records=True)
        def records(_):
            return (
                {
                    'column1': 0,
                    'column2': 1
                },
                {
                    'column1': 2,
                    'column2': 3
                },
            )

        cls.records_to_df = records
        cls.students_table = Students.__table__
        cls.array_and_columns_to_df = array_and_columns

        # with python 3.6+ dictionary order of insertion is ok
        cls.table_inspect = {
            'table_name': 'students',
            'row_count': 'N/A',
            'schema': None,
            'columns': [
                {
                    'column_name': 'first_name',
                    'column_type': 'VARCHAR(20)',
                    'python_type': str,
                    'primary_key': True,
                    'nullable': False
                },
                {
                    'column_name': 'last_name',
                    'column_type': 'VARCHAR(20)',
                    'python_type': str,
                    'primary_key': True,
                    'nullable': False
                },
                {
                    'column_name': 'middle_name',
                    'column_type': 'VARCHAR(20)',
                    'python_type': str,
                    'primary_key': False,
                    'nullable': True
                },
                {
                    'column_name': 'gender',
                    'column_type': 'VARCHAR(1)',
                    'python_type': str,
                    'primary_key': False,
                    'nullable': True
                },
                {
                    'column_name': 'age',
                    'column_type': 'INTEGER',
                    'python_type': int,
                    'primary_key': False,
                    'nullable': False
                }
            ]
        }

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_dframe(self):
        self.assertIsInstance(self.array_and_columns_to_df(), pd.DataFrame)
        self.assertEqual(self.array_and_columns_to_df().shape, (10, 2))

        self.assertIsInstance(self.records_to_df(), pd.DataFrame)
        self.assertEqual(self.records_to_df().shape, (2, 2))

    def test_inspect_table(self):
        inspection = inspect_table(self.students_table)

        self.assertEqual(set(inspection.keys()), set(self.table_inspect.keys()))
        self.assertEqual(len(inspection.get('columns')), len(self.table_inspect.get('columns')))

        for key in ('table_name', 'row_count', 'schema'):
            with self.subTest(key=key):
                self.assertEqual(inspection.get(key), self.table_inspect.get(key))

        for i in range(len(inspection.get('columns'))):
            with self.subTest(i=i):
                for key in ('column_name', 'column_type', 'python_type', 'primary_key', 'nullable',):
                    with self.subTest(key=key):
                        self.assertEqual(
                            inspection.get('columns')[i].get(key),
                            self.table_inspect.get('columns')[i].get(key)
                        )

        # Todo: check exception

    def test_filter_maker(self):
        self.assertIsInstance(
            filter_maker(self.students_table, 'age', 10),
            sqlelements.BinaryExpression
        )
        self.assertTrue(
            filter_maker(self.students_table, 'age', 10).compare(
                self.students_table.columns.age == 10
            )
        )


if __name__ == '__main__':
    unittest.main()
