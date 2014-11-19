from unittest import TestCase, main

from psycopg2._psycopg import connection, cursor
from psycopg2 import connect, ProgrammingError
from psycopg2.extensions import (ISOLATION_LEVEL_AUTOCOMMIT,
                                 ISOLATION_LEVEL_READ_COMMITTED)

from gd import gd_config
from gd.sql_connection import SQLConnectionHandler
from gd.exceptions import GDExecutionError, GDConnectionError


DB_LAYOUT = """CREATE TABLE test_table (
    str_column           varchar  DEFAULT 'foo' NOT NULL,
    bool_column          bool DEFAULT True NOT NULL,
    int_column           bigint NOT NULL
);"""


class TestConnHandler(TestCase):
    def setUp(self):
        # First check that we are connected to the test database, so we are
        # sure that we are not destroying anything
        if gd_config.database != "sql_handler_test":
            raise RuntimeError(
                "Not running the tests since the system is not connected to "
                "the test database 'sql_handler_test'")

        # Destroy the test database and create it again, so the tests are
        # independent and the test database is always available
        with connect(user=gd_config.admin_user,
                     password=gd_config.admin_password, host=gd_config.host,
                     port=gd_config.port) as con:
            # Set the isolation level to autocommit so we can drop the database
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with con.cursor() as cur:
                try:
                    cur.execute("DROP DATABASE sql_handler_test")
                except ProgrammingError:
                    # Means that the sql_handler_test database does not exist
                    # This will happen on test_init_connection_error
                    pass

                # Create the database again
                cur.execute("CREATE DATABASE sql_handler_test")

        with connect(user=gd_config.user, password=gd_config.password,
                     host=gd_config.host, port=gd_config.port,
                     database=gd_config.database) as con:
            with con.cursor() as cur:
                cur.execute(DB_LAYOUT)

        # Instantiate a conn_handler for the tests
        self.conn_handler = SQLConnectionHandler()

    def tearDown(self):
        # We need to delete the conn_handler, so the connection is closed
        del self.conn_handler

    def _populate_test_table(self):
        sql = ("INSERT INTO test_table (str_column, bool_column, int_column) "
               "VALUES (%s, %s, %s)")
        sql_args = [('test1', True, 1), ('test2', True, 2),
                    ('test3', False, 3), ('test4', False, 4)]
        con = connect(user=gd_config.user, password=gd_config.password,
                      host=gd_config.host, port=gd_config.port,
                      database=gd_config.database)
        with con.cursor() as cur:
            cur.executemany(sql, sql_args)
        con.commit()
        con.close()

    def _assert_sql_equal(self, exp):
        con = connect(user=gd_config.user, password=gd_config.password,
                      host=gd_config.host, port=gd_config.port,
                      database=gd_config.database)
        with con.cursor() as cur:
            cur.execute("SELECT * FROM test_table")
            obs = cur.fetchall()
        con.commit()
        con.close()

        self.assertEqual(obs, exp)

    def test_init(self):
        """init successfully initializes the handler"""
        obs = SQLConnectionHandler()
        self.assertEqual(obs.admin, 'no_admin')
        self.assertEqual(obs.queues, {})
        self.assertTrue(isinstance(obs._connection, connection))

    def test_init_admin_error(self):
        """Init raises an error if admin is an unrecognized value"""
        with self.assertRaises(RuntimeError):
            SQLConnectionHandler(admin='not a valid value')

    def test_init_connection_error(self):
        """init raises an error if cannot connect to the database"""
        # We first need to close all the connexions
        self.conn_handler._connection.close()
        # In order to force a connection failure, remove the test database
        with connect(user=gd_config.admin_user,
                     password=gd_config.admin_password, host=gd_config.host,
                     port=gd_config.port) as con:
            # Set the isolation level to autocommit so we can drop the database
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with con.cursor() as cur:
                cur.execute("DROP DATABASE sql_handler_test")

        with self.assertRaises(GDConnectionError):
            SQLConnectionHandler()

    def test_set_autocommit(self):
        """set_autocommit correctly activates/deactivates the autocommit"""
        self.assertEqual(self.conn_handler._connection.isolation_level,
                         ISOLATION_LEVEL_READ_COMMITTED)
        self.conn_handler.set_autocommit('on')
        self.assertEqual(self.conn_handler._connection.isolation_level,
                         ISOLATION_LEVEL_AUTOCOMMIT)
        self.conn_handler.set_autocommit('off')
        self.assertEqual(self.conn_handler._connection.isolation_level,
                         ISOLATION_LEVEL_READ_COMMITTED)

    def test_set_autocommit_error(self):
        """set_autocommit raises an error if the parameter is not 'on' or 'off'
        """
        with self.assertRaises(ValueError):
            self.conn_handler.set_autocommit('not a valid value')

    def test_check_sql_args(self):
        """check_sql_args returns the execution to the caller if type is ok"""
        self.conn_handler._check_sql_args(['a', 'list'])
        self.conn_handler._check_sql_args(('a', 'tuple'))
        self.conn_handler._check_sql_args({'a': 'dict'})
        self.conn_handler._check_sql_args(None)

    def test_check_sql_args_error(self):
        """check_sql_args raises an error with unsupported types"""
        with self.assertRaises(TypeError):
            self.conn_handler._check_sql_args("a string")

        with self.assertRaises(TypeError):
            self.conn_handler._check_sql_args(1)

        with self.assertRaises(TypeError):
            self.conn_handler._check_sql_args(1.2)

    def test_sql_executor_no_sql_args(self):
        """sql_executor works with no sql arguments"""
        sql = "INSERT INTO test_table (int_column) VALUES (1)"
        with self.conn_handler._sql_executor(sql) as cur:
            self.assertTrue(cur, cursor)

        self._assert_sql_equal([('foo', True, 1)])

    def test_sql_executor_with_sql_args(self):
        """sql_executor works with sql arguments"""
        sql = "INSERT INTO test_table (int_column) VALUES (%s)"
        with self.conn_handler._sql_executor(sql, sql_args=(1,)) as cur:
            self.assertTrue(cur, cursor)

        self._assert_sql_equal([('foo', True, 1)])

    def test_sql_executor_many(self):
        """sql_executor works with many"""
        sql = "INSERT INTO test_table (int_column) VALUES (%s)"
        sql_args = [(1,), (2,)]
        with self.conn_handler._sql_executor(sql, sql_args=sql_args,
                                             many=True) as cur:
            self.assertTrue(cur, cursor)

        self._assert_sql_equal([('foo', True, 1), ('foo', True, 2)])

    def test_execute_no_sql_args(self):
        """execute works with no arguments"""
        sql = "INSERT INTO test_table (int_column) VALUES (1)"
        self.conn_handler.execute(sql)

        self._assert_sql_equal([('foo', True, 1)])

    def test_execute_with_sql_args(self):
        """execute works with arguments"""
        sql = "INSERT INTO test_table (int_column) VALUES (%s)"
        self.conn_handler.execute(sql, (1,))

        self._assert_sql_equal([('foo', True, 1)])

    def test_executemany(self):
        """executemany works as expected"""
        sql = "INSERT INTO test_table (int_column) VALUES (%s)"
        self.conn_handler.executemany(sql, [(1,), (2,)])

        self._assert_sql_equal([('foo', True, 1), ('foo', True, 2)])

    def test_execute_fetchone_no_sql_args(self):
        """execute_fetchone works with no arguments"""
        self._populate_test_table()

        sql = "SELECT str_column FROM test_table WHERE int_column = 1"
        obs = self.conn_handler.execute_fetchone(sql)

        self.assertEqual(obs, ['test1'])

    def test_execute_fetchone_with_sql_args(self):
        """execute_fetchone works with arguments"""
        self._populate_test_table()

        sql = "SELECT str_column FROM test_table WHERE int_column = %s"
        obs = self.conn_handler.execute_fetchone(sql, (2,))

        self.assertEqual(obs, ['test2'])

    def test_execute_fetchall_no_sql_args(self):
        """execute_fetchall works with no arguments"""
        self._populate_test_table()

        sql = "SELECT * FROM test_table WHERE bool_column = False"
        obs = self.conn_handler.execute_fetchall(sql)

        self.assertEqual(obs, [['test3', False, 3], ['test4', False, 4]])

    def test_execute_fetchall_with_sql_args(self):
        """execute_fetchall works with arguments"""
        self._populate_test_table()

        sql = "SELECT * FROM test_table WHERE bool_column = %s"
        obs = self.conn_handler.execute_fetchall(sql, (True, ))

        self.assertEqual(obs, [['test1', True, 1], ['test2', True, 2]])

    def test_create_queue(self):
        """create_queue initializes a new queue"""
        self.assertEqual(self.conn_handler.queues, {})
        self.conn_handler.create_queue("test_queue")
        self.assertEqual(self.conn_handler.queues, {'test_queue': []})

    def test_create_queue_error(self):
        """create_queue raises an error if the queue already exists"""
        self.conn_handler.create_queue("test_queue")
        with self.assertRaises(KeyError):
            self.conn_handler.create_queue("test_queue")

    def test_list_queues(self):
        """test_list_queues works correctly"""
        self.assertEqual(self.conn_handler.list_queues(), [])
        self.conn_handler.create_queue("test_queue")
        self.assertEqual(self.conn_handler.list_queues(), ["test_queue"])

    def test_add_to_queue(self):
        """add_to_queue works correctly"""
        self.conn_handler.create_queue("test_queue")
        self.assertEqual(self.conn_handler.queues, {"test_queue": []})

        sql1 = "INSERT INTO test_table (int_column) VALUES (%s)"
        sql_args1 = (1,)
        self.conn_handler.add_to_queue("test_queue", sql1, sql_args1)
        self.assertEqual(self.conn_handler.queues,
                         {"test_queue": [(sql1, sql_args1)]})

        sql2 = "INSERT INTO test_table (int_column) VALUES (2)"
        self.conn_handler.add_to_queue("test_queue", sql2)
        self.assertEqual(self.conn_handler.queues,
                         {"test_queue": [(sql1, sql_args1), (sql2, None)]})

    def test_add_to_queue_many(self):
        """add_to_queue works with many"""
        self.conn_handler.create_queue("test_queue")
        self.assertEqual(self.conn_handler.queues, {"test_queue": []})

        sql = "INSERT INTO test_table (int_column) VALUES (%s)"
        sql_args = [(1,), (2,), (3,)]
        self.conn_handler.add_to_queue("test_queue", sql, sql_args, many=True)
        self.assertEqual(self.conn_handler.queues,
                         {"test_queue": [(sql, (1,)), (sql, (2,)),
                                         (sql, (3,))]})

    def test_execute_queue(self):
        self.conn_handler.create_queue("test_queue")
        self.conn_handler.add_to_queue(
            "test_queue",
            "INSERT INTO test_table (str_column, int_column) VALUES (%s, %s)",
            ['test_insert', '2'])
        self.conn_handler.add_to_queue(
            "test_queue",
            "UPDATE test_table SET int_column = 20, bool_column = FALSE "
            "WHERE str_column = %s",
            ['test_insert'])
        obs = self.conn_handler.execute_queue("test_queue")
        self.assertEqual(obs, [])

        self._assert_sql_equal([('test_insert', False, 20)])

    def test_execute_queue_many(self):
        sql = "INSERT INTO test_table (str_column, int_column) VALUES (%s, %s)"
        sql_args = [('insert1', 1), ('insert2', 2), ('insert3', 3)]

        self.conn_handler.create_queue("test_queue")
        self.conn_handler.add_to_queue("test_queue", sql, sql_args, many=True)
        self.conn_handler.add_to_queue(
            "test_queue",
            "UPDATE test_table SET int_column = 20, bool_column = FALSE "
            "WHERE str_column = %s",
            ['insert2'])
        obs = self.conn_handler.execute_queue('test_queue')
        self.assertEqual(obs, [])

        self._assert_sql_equal([('insert1', True, 1), ('insert3', True, 3),
                                ('insert2', False, 20)])

    def test_execute_queue_last_return(self):
        self.conn_handler.create_queue("test_queue")
        self.conn_handler.add_to_queue(
            "test_queue",
            "INSERT INTO test_table (str_column, int_column) VALUES (%s, %s)",
            ['test_insert', '2'])
        self.conn_handler.add_to_queue(
            "test_queue",
            "UPDATE test_table SET bool_column = FALSE WHERE str_column = %s "
            "RETURNING int_column",
            ['test_insert'])
        obs = self.conn_handler.execute_queue("test_queue")
        self.assertEqual(obs, [2])

    def test_execute_queue_placeholders(self):
        self.conn_handler.create_queue("test_queue")
        self.conn_handler.add_to_queue(
            "test_queue",
            "INSERT INTO test_table (int_column) VALUES (%s) "
            "RETURNING str_column", (2,))
        self.conn_handler.add_to_queue(
            "test_queue",
            "UPDATE test_table SET bool_column = FALSE WHERE str_column = %s",
            ('{0}',))
        obs = self.conn_handler.execute_queue("test_queue")
        self.assertEqual(obs, [])

        self._assert_sql_equal([('foo', False, 2)])

    def test_queue_fail(self):
        """Fail if no results data exists for substitution"""
        self.conn_handler.create_queue("test_queue")
        self.conn_handler.add_to_queue(
            "test_queue",
            "INSERT INTO test_table (int_column) VALUES (%s)", (2,))
        self.conn_handler.add_to_queue(
            "test_queue",
            "UPDATE test_table SET bool_column = FALSE WHERE str_column = %s",
            ('{0}',))

        with self.assertRaises(GDExecutionError):
            self.conn_handler.execute_queue("test_queue")

        # make sure rollback correctly
        self._assert_sql_equal([])

    def test_huge_queue(self):
        self.conn_handler.create_queue("test_queue")
        # add tons of inserts to queue
        for x in range(120):
            self.conn_handler.add_to_queue(
                "test_queue",
                "INSERT INTO test_table (int_column) VALUES (%s)", (x,))

        # add failing insert as final item in queue
        self.conn_handler.add_to_queue(
            "test_queue",
            "INSERT INTO NO_TABLE (some_column) VALUES (1)")

        with self.assertRaises(GDExecutionError):
            self.conn_handler.execute_queue("test_queue")

        # make sure rollback correctly
        self._assert_sql_equal([])


if __name__ == "__main__":
    main()
