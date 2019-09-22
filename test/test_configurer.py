import json
import pathlib
import unittest
import tempfile
import contextlib
from dsdbmanager.configuring import ConfigFilesManager


class TestConfigurer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key = b'bt5cet5XS4Zemk0gMcFGCwuCvw4kqwsqGvnr8NYpPBs='
        cls.pwd = b'mypassword'

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
                    'sid': 'pyt'
                }
            }
        }

        cls.cred = {
            'oracle': {
                'mydatabase': {
                    'username': 'some_username',
                    'password': 'some_password'
                },
                'newdatabase': {
                    'username': 'some_username',
                    'password': 'some_password'
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
        pass

    def tearDown(self):
        pass

    def test_generate_key(self):
        # note that we do not care about the path here
        c = ConfigFilesManager(
            pathlib.Path(''),
            pathlib.Path(''),
            pathlib.Path('')
        )
        self.assertIsInstance(c.generate_key(), bytes)

    def test_get_hosts(self):
        with self.generate_path(suffix='.json') as hp, self.generate_path(
                suffix='.json') as cp, self.generate_path() as kp:
            c = ConfigFilesManager(
                hp,
                cp,
                kp
            )

            # without actual host data, it will just be empty,
            self.assertEqual({}, c.get_hosts())

            # write data and read to check
            try:
                with hp.open('w') as f:
                    json.dump(self.host, f)

                host_data = c.get_hosts()

                self.assertIsInstance(host_data, dict)
                self.assertEqual(self.host, host_data)
            except OSError:
                pass

    def test_encrypt_decrypt(self):
        with self.generate_path(suffix='.json') as hp, self.generate_path(
                suffix='.json') as cp, self.generate_path() as kp:
            c = ConfigFilesManager(
                hp,
                cp,
                kp
            )

            # write key to the path
            kp.write_bytes(self.key)

            # encrypt and check type
            encrypted_pwd = c.encrypt_decrypt(self.pwd, True)
            self.assertIsInstance(encrypted_pwd, bytes)

            # decrypt and compare
            decrypted_pwd = c.encrypt_decrypt(encrypted_pwd, False)
            self.assertIsInstance(decrypted_pwd, bytes)
            self.assertEqual(decrypted_pwd, self.pwd)

    def test_read_credentials(self):
        with self.generate_path(suffix='.json') as hp, self.generate_path(
                suffix='.json') as cp, self.generate_path() as kp:
            c = ConfigFilesManager(
                hp,
                cp,
                kp
            )

            # read credential on empty data. should be null
            usr, pwd = c.read_credentials('oracle', 'mydatabase')
            self.assertIsNone(usr)
            self.assertIsNone(pwd)

            # write credentials and read
            with cp.open('w') as f:
                json.dump(self.cred, f)

            username, password = c.read_credentials('oracle', 'mydatabase')
            self.assertIsInstance(username, bytes)
            self.assertIsInstance(password, bytes)
            self.assertEqual(username.decode(), self.cred['oracle']['mydatabase']['username'])
            self.assertEqual(password.decode(), self.cred['oracle']['mydatabase']['password'])

            # now since there is no mssql for example there should be None
            username, password = c.read_credentials('mssql', 'somedatabase')
            self.assertIsNone(username)
            self.assertIsNone(password)

    def test_write_credentials(self):
        with self.generate_path(suffix='.json') as hp, self.generate_path(
                suffix='.json') as cp, self.generate_path() as kp:
            c = ConfigFilesManager(
                hp,
                cp,
                kp
            )

            # trying to write credentials when the path is not even initialized to {}
            with self.assertRaises(json.JSONDecodeError):
                _ = c.write_credentials('mssql', 'somedb', b'user', b'pass')

            # initialize
            with cp.open('w') as f:
                json.dump({}, f)

            _ = c.write_credentials('mssql', 'somedb', b'user', b'pass')

            expected = {
                'mssql': {
                    'somedb': {
                        'username': 'user',
                        'password': 'pass'
                    }
                }
            }

            with cp.open() as f:
                written = json.load(f)

            self.assertEqual(written, expected)

    def test_remove_credential(self):
        with self.generate_path(suffix='.json') as hp, self.generate_path(
                suffix='.json') as cp, self.generate_path() as kp:
            c = ConfigFilesManager(
                hp,
                cp,
                kp
            )

            # trying to remove anything without the initial {} should cause problems
            with self.assertRaises(json.JSONDecodeError):
                _ = c._remove_credential('mssql', 'somedb')

            # initialize the credential data for oracle
            with cp.open('w') as f:
                json.dump(self.cred, f)

            # remove one database and check one is left
            _ = c._remove_credential('oracle', 'mydatabase')

            with cp.open() as f:
                remaining = json.load(f)

            expected = {
                'oracle': {
                    'newdatabase': {
                        'username': 'some_username',
                        'password': 'some_password'
                    }
                }
            }
            self.assertEqual(remaining, expected)


if __name__ == '__main__':
    unittest.main()
