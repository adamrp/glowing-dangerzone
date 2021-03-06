r"""
SQL Connection object (:mod:`gd.sql_connection`)
================================================

.. currentmodule:: gd.sql_connection

This modules provides wrappers for the psycopg2 module to allow easy use of
transaction blocks and SQL execution/data retrieval.

Classes
-------

.. autosummary::
   :toctree: generated/

   SQLConnectionHandler

Examples
--------
Transaction blocks are created by first creating a queue of SQL commands, then
adding commands to it. Finally, the execute command is called to execute the
entire queue of SQL commands. A single command is made up of SQL and sql_args.
SQL is the sql string in psycopg2 format with \%s markup, and sql_args is the
list or tuple of replacement items.
An example of a basic queue with two SQL commands in a single transaction:

>>> from gd.sql_connection import SQLConnectionHandler
>>> conn_handler = SQLConnectionHandler # doctest: +SKIP
>>> conn_handler.create_queue("example_queue") # doctest: +SKIP
>>> conn_handler.add_to_queue(
...     "example_queue", "INSERT INTO user (email, name, password,"
...     "phone) VALUES (%s, %s, %s, %s)",
...     ['insert@foo.bar', 'Toy', 'pass', '111-111-11112']) # doctest: +SKIP
>>> conn_handler.add_to_queue(
...     "example_queue", "UPDATE user SET user_level_id = 1, "
...     "phone = '222-222-2221' WHERE email = %s",
...     ['insert@foo.bar']) # doctest: +SKIP
>>> conn_handler.execute_queue("example_queue") # doctest: +SKIP
>>> conn_handler.execute_fetchall(
...     "SELECT * FROM user WHERE email = %s",
...     ['insert@foo.bar']) # doctest: +SKIP
[['insert@foo.bar', 1, 'pass', 'Toy', None, None, '222-222-2221', None, None,
  None]] # doctest: +SKIP

You can also use results from a previous command in the queue in a later
command. If an item in the queue depends on a previous sql command's output,
use {#} notation as a placeholder for the value. The \# must be the
position of the result, e.g. if you return two things you can use \{0\}
to reference the first and \{1\} to referece the second. The results list
will continue to grow until one of the references is reached, then it
will be cleaned out.
Modifying the previous example to show this ability (Note the RETURNING added
to the first SQL command):

>>> from gd.sql_connection import SQLConnectionHandler
>>> conn_handler = SQLConnectionHandler # doctest: +SKIP
>>> conn_handler.create_queue("example_queue") # doctest: +SKIP
>>> conn_handler.add_to_queue(
...     "example_queue", "INSERT INTO user (email, name, password,"
...     "phone) VALUES (%s, %s, %s, %s) RETURNING email, password",
...     ['insert@foo.bar', 'Toy', 'pass', '111-111-11112']) # doctest: +SKIP
>>> conn_handler.add_to_queue(
...     "example_queue", "UPDATE user SET user_level_id = 1, "
...     "phone = '222-222-2221' WHERE email = %s AND password = %s",
...     ['{0}', '{1}']) # doctest: +SKIP
>>> conn_handler.execute_queue("example_queue") # doctest: +SKIP
>>> conn_handler.execute_fetchall(
...     "SELECT * from user WHERE email = %s",
...     ['insert@foo.bar']) # doctest: +SKIP
[['insert@foo.bar', 1, 'pass', 'Toy', None, None, '222-222-2221', None, None,
  None]] # doctest: +SKIP
"""
# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The biocore Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------
from __future__ import division
from contextlib import contextmanager
from functools import partial
from itertools import chain

from psycopg2 import connect, ProgrammingError, Error as PostgresError
from psycopg2.extras import DictCursor
from psycopg2.extensions import (ISOLATION_LEVEL_AUTOCOMMIT,
                                 ISOLATION_LEVEL_READ_COMMITTED)

from gd import gd_config
from gd.exceptions import GDExecutionError, GDConnectionError

INIT_ADMIN_OPTS = {'no_admin', 'admin_with_database', 'admin_without_database'}


def flatten(list_of_lists):
    # https://docs.python.org/2/library/itertools.html
    return chain.from_iterable(list_of_lists)


class SQLConnectionHandler(object):
    """Encapsulates the DB connection with the Postgres DB

    Parameters
    ----------
    admin : {0}, optional
        Whether or not to connect as the admin user. Options other than
        `no_admin` depend on admin credentials in the glowing-dangerzone
        configuration. If 'admin_without_database', the connection will be made
        to the server specified in the gd configuration, but not to a
        specific database. If 'admin_with_database', then a connection will be
        made to the server and database specified in the gd config.
    """.format(INIT_ADMIN_OPTS)

    def __init__(self, admin='no_admin'):
        if admin not in INIT_ADMIN_OPTS:
            raise GDConnectionError(
                "admin takes only on of %s" % INIT_ADMIN_OPTS)

        self.admin = admin
        self._connection = None
        self._open_connection()
        # queues for transaction blocks. Format is {str: list} where the str
        # is the queue name and the list is the queue of SQL commands
        self.queues = {}

    def __del__(self):
        # Close the connection only if it is not already closed
        try:
            if self._connection and not self._connection.closed:
                self._connection.close()
        except AttributeError:
            # There was an issue initializing the connection attribute and
            # it does not exist
            pass

    def _open_connection(self):
        # connection string arguments for a normal user
        args = {
            'user': gd_config.user,
            'password': gd_config.password,
            'database': gd_config.database,
            'host': gd_config.host,
            'port': gd_config.port}

        # if this is an admin user, use the admin credentials
        if self.admin != 'no_admin':
            args['user'] = gd_config.admin_user
            args['password'] = gd_config.admin_password

        # Do not connect to a particular database unless requested
        if self.admin == 'admin_without_database':
            del args['database']

        try:
            self._connection = connect(**args)
        except Exception as e:
            # catch any exception and raise as runtime error
            raise GDConnectionError("Cannot connect to database: %s" % str(e))

    @contextmanager
    def get_postgres_cursor(self):
        """ Returns a Postgres cursor

        Returns
        -------
        pgcursor : psycopg2.cursor

        Raises a GDConnectionError if the cursor cannot be created
        """
        if self._connection.closed:
            self._open_connection()

        try:
            with self._connection.cursor(cursor_factory=DictCursor) as cur:
                yield cur
        except PostgresError as e:
            raise GDConnectionError("Error running query: %s" % e)

    @property
    def autocommit(self):
        return self._connection.isolation_level == ISOLATION_LEVEL_AUTOCOMMIT

    @autocommit.setter
    def autocommit(self, value):
        if not isinstance(value, bool):
            raise TypeError('The value for autocommit should be a boolean')
        level = (ISOLATION_LEVEL_AUTOCOMMIT if value
                 else ISOLATION_LEVEL_READ_COMMITTED)
        self._connection.set_isolation_level(level)

    def _check_sql_args(self, sql_args):
        """Checks that sql_args have the correct type

        Parameters
        ----------
        sql_args : object
            The SQL arguments

        Returns
        -------
        None

        Raises
        ------
        TypeError
            if sql_args does not have the correct type
        """
        # Check that sql arguments have the correct type
        if sql_args and type(sql_args) not in [tuple, list, dict]:
            raise TypeError("sql_args should be tuple, list or dict. Found %s "
                            % type(sql_args))

    @contextmanager
    def _sql_executor(self, sql, sql_args=None, many=False):
        """Executes an SQL query

        Parameters
        ----------
        sql : str
            The SQL query
        sql_args : tuple or list, optional
            The arguments for the SQL query
        many : bool, optional
            If true, performs an execute many call

        Returns
        -------
        pgcursor : psycopg2.cursor
            The cursor in which the SQL query was executed

        Raises
        ------
        GDExecutionError
            If there is some error executing the SQL query
        """
        # Check that sql arguments have the correct type
        if many:
            for args in sql_args:
                self._check_sql_args(args)
        else:
            self._check_sql_args(sql_args)

        # Execute the query
        with self.get_postgres_cursor() as cur:
            execute = partial(cur.executemany if many else cur.execute,
                              sql, sql_args)
            try:
                execute()
                yield cur
            except PostgresError as e:
                self._connection.rollback()
                raise GDExecutionError(("\nError running SQL query: %s"
                                        "\nARGS: %s"
                                        "\nError: %s" %
                                        (sql, str(sql_args), e)))
            else:
                self._connection.commit()

    def execute(self, sql, sql_args=None):
        """ Executes an SQL query with no results

        Parameters
        ----------
        sql : str
            The SQL query
        sql_args : tuple or list, optional
            The arguments for the SQL query

        Raises
        ------
        GDExecutionError
            if there is some error executing the SQL query

        Notes
        -----
        From psycopg2 documentation, only variable values should be bound via
        sql_args, it shouldn't be used to set table or field names. For those
        elements, ordinary string formatting should be used before running
        execute.
        """
        with self._sql_executor(sql, sql_args):
            pass

    def executemany(self, sql, sql_args_list):
        """ Executes an executemany SQL query with no results

        Parameters
        ----------
        sql : str
            The SQL query
        sql_args : list of tuples
            The arguments for the SQL query

        Raises
        ------
        GDExecutionError
            If there is some error executing the SQL query

        Notes
        -----
        From psycopg2 documentation, only variable values should be bound via
        sql_args, it shouldn't be used to set table or field names. For those
        elements, ordinary string formatting should be used before running
        execute.
        """
        with self._sql_executor(sql, sql_args_list, True):
            pass

    def execute_fetchone(self, sql, sql_args=None):
        """ Executes a fetchone SQL query

        Parameters
        ----------
        sql : str
            The SQL query
        sql_args : tuple or list, optional
            The arguments for the SQL query

        Returns
        -------
        Tuple
            The results of the fetchone query

        Raises
        ------
        GDExecutionError
            if there is some error executing the SQL query

        Notes
        -----
        From psycopg2 documentation, only variable values should be bound via
        sql_args, it shouldn't be used to set table or field names. For those
        elements, ordinary string formatting should be used before running
        execute.
        """
        with self._sql_executor(sql, sql_args) as pgcursor:
            result = pgcursor.fetchone()
        return result

    def execute_fetchall(self, sql, sql_args=None):
        """ Executes a fetchall SQL query

        Parameters
        ----------
        sql : str
            The SQL query
        sql_args : tuple or list, optional
            The arguments for the SQL query

        Returns
        ------
        list of tuples
            The results of the fetchall query

        Raises
        ------
        GDExecutionError
            If there is some error executing the SQL query

        Notes
        -----
        From psycopg2 documentation, only variable values should be bound via
        sql_args, it shouldn't be used to set table or field names. For those
        elements, ordinary string formatting should be used before running
        execute.
        """
        with self._sql_executor(sql, sql_args) as pgcursor:
            result = pgcursor.fetchall()
        return result

    def _check_queue_exists(self, queue_name):
        if queue_name not in self.queues:
            raise KeyError("Queue %s does not exists" % queue_name)

    def create_queue(self, queue_name):
        """Add a new queue to the connection

        Parameters
        ----------
        queue_name : str
            Name of the new queue

        Raises
        ------
        KeyError
            Queue name already exists
        """
        if queue_name in self.queues:
            raise KeyError("Queue already contains %s" % queue_name)

        self.queues[queue_name] = []

    def list_queues(self):
        """Returns list of all queue names currently in handler

        Returns
        -------
        list
            names of queues in handler
        """
        return self.queues.keys()

    def add_to_queue(self, queue, sql, sql_args=None, many=False):
        """Add an sql command to the end of a queue

        Parameters
        ----------
        queue : str
            name of queue adding to
        sql : str
            sql command to run
        sql_args : list or tuple, optional
            the arguments to fill sql command with
        many : bool, optional
            Whether or not this should be treated as an executemany command.
            Default False

        Raises
        ------
        KeyError
            queue does not exist

        Notes
        -----
        Queues are executed in FIFO order
        """
        self._check_queue_exists(queue)

        if not many:
            sql_args = [sql_args]

        for args in sql_args:
            self._check_sql_args(args)
            self.queues[queue].append((sql, args))

    def _rollback_raise_error(self, queue, sql, sql_args, e):
        self._connection.rollback()
        # wipe out queue since it has an error in it
        del self.queues[queue]
        raise GDExecutionError(
            "\nError running SQL query in queue %s: %s\nARGS: %s\nError: %s"
            % (queue, sql, str(sql_args), e))

    def execute_queue(self, queue):
        """Executes all sql in a queue in a single transaction block

        Parameters
        ----------
        queue : str
            Name of queue to execute

        Notes
        -----
        Does not support executemany command. Instead, enter the multiple
        SQL commands as multiple entries in the queue.

        Queues are executed in FIFO order
        """
        self._check_queue_exists(queue)

        with self.get_postgres_cursor() as cur:
            results = []
            clear_res = False
            for sql, sql_args in self.queues[queue]:
                if sql_args is not None:
                    # The user can provide a tuple, make sure that it
                    # is a list, so we can assign the item
                    sql_args = list(sql_args)
                    for pos, arg in enumerate(sql_args):
                        # check if previous results needed and replace
                        if isinstance(arg, str) and \
                                arg[0] == "{" and arg[-1] == "}":
                            result_pos = int(arg[1:-1])
                            try:
                                sql_args[pos] = results[result_pos]
                            except IndexError:
                                raise GDExecutionError(
                                    "The index provided as a placeholder does "
                                    "not correspond to any previous result")
                            clear_res = True
                # wipe out results if needed and reset clear_res
                if clear_res:
                    results = []
                    clear_res = False
                # Fire off the SQL command
                try:
                    cur.execute(sql, sql_args)
                except Exception as e:
                    self._rollback_raise_error(queue, sql, sql_args, e)

                # fetch results if available and append to results list
                try:
                    res = cur.fetchall()
                except ProgrammingError as e:
                    # At this execution point, we don't know if the sql query
                    # that we executed was a INSERT or a SELECT. If it was a
                    # SELECT and there is nothing to fetch, it will return an
                    # empty list. However, if it was a INSERT it will raise a
                    # ProgrammingError, so we catch that one and pass.
                    pass
                except PostgresError as e:
                    self._rollback_raise_error(queue, sql, sql_args, e)
                else:
                    # append all results linearly
                    results.extend(flatten(res))
        self._connection.commit()
        # wipe out queue since finished
        del self.queues[queue]
        return results
