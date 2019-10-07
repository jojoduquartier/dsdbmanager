import unittest
from dsdbmanager.mssql_ import Mssql
from dsdbmanager.mysql_ import Mysql
from dsdbmanager.oracle_ import Oracle
from dsdbmanager.teradata_ import Teradata
from dsdbmanager.exceptions_ import MissingFlavor, MissingDatabase, MissingPackage


class TestConnectors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.host_dict = {
            'oracle': {
                'database1': {
                    'name': 'database1',
                    'host': 'somehost',
                    'port': 0000
                }
            },
            'teradata': {
                'database1': {
                    'name': 'database1',
                    'host': 'somehost',
                }
            },
            'mssql': {
                'database1': {
                    'name': 'database1',
                    'host': 'somehost',
                    'port': 0000
                }
            },
            'mysql': {
                'database1': {
                    'name': 'database1',
                    'host': 'somehost',
                    'port': 0000
                }
            }
        }

    @classmethod
    def tearDownClass(cls):
        pass

    def test_connectors(self):
        for name, connector in zip(
                [
                    'oracle',
                    'mysql',
                    'mssql',
                    'teradata',
                ],
                [
                    Oracle,
                    Mysql,
                    Mssql,
                    Teradata,
                ]):
            with self.subTest(flavor=name):
                # test with host improper host file. should raise MissingFlavor
                with self.assertRaises(MissingFlavor):
                    _ = connector('database1', {'host': 'dummy'})

                # test for database that has not been added
                with self.assertRaises(MissingDatabase):
                    _ = connector('database2', self.host_dict)

                # test for database added
                obj = connector('database1', self.host_dict)
                self.assertTrue(hasattr(obj, 'create_engine'))

                # this is only useful in an environment where none of the extra packages are available
                # with self.assertRaises(MissingPackage):
                #     obj.create_engine()


if __name__ == '__main__':
    unittest.main()
