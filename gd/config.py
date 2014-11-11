# -----------------------------------------------------------------------------
# Copyright (c) 2014--, The biocore Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

import os
from future import standard_library
with standard_library.hooks():
    from configparser import ConfigParser


class GDConfig(object):
    """Holds the glowing-dangerzone configuration

    Attributes
    ----------
    user : str
        The postgres user to connect to the postgres server
    password : str
        The password for the previous user
    database : str
        The database to connect to
    host : str
        The host where the postgres server lives
    port : str
        The port to use to connect to the postgres server
    admin_user : str
        The administrator user to connect to the postgres server
    admin_password : str
        The password for the administrator user
    """

    def __init__(self):
        conf_fp = os.environ['GD_CONFIG_FP']
        # parse the config bits
        config = ConfigParser()
        with open(conf_fp) as f:
            config.readfp(f)

        self.user = config.get('postgres', 'USER')
        self.password = config.get('postgres', 'PASSWORD') or None
        self.database = config.get('postgres', 'DATABASE')
        self.host = config.get('postgres', 'HOST')
        self.port = config.getint('postgres', 'PORT')
        self.admin_user = config.get('postgres', 'ADMIN_USER') or None
        self.admin_password = config.get('postgres', 'ADMIN_PASSWORD') or None
