# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The biocore Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------


class GDError(Exception):
    """Base class for all glowing-dangerzone exceptions"""
    pass


class GDConnectionError(GDError):
    """Exception for error when connecting to the db"""
    pass


class GDExecutionError(GDError):
    """Exception for error when executing SQL queries"""
    pass
