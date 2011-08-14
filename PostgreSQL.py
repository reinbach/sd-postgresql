#!/usr/bin/env python

import re
import commands

PLUGIN_NAME = 'PostgreSQL Plugin'
CONFIG_PARAMS = [
    # ('config key', 'name', 'required'),
    ('postgres_database', 'PostgreSQLDatabase', True),
    ('postgres_user', 'PostgreSQLUser', True),
    ('postgres_pass', 'PostgreSQLPassword', True),
    ('postgres_port', 'PostgreSQLPort', False),
]
PLUGIN_STATS = [
    'postgresVersion',
    'postgresMaxConnections',
    'postgresCurrentConnections',
    'postgresLocks',
    'postgresLogFile',
]

#===============================================================================
class PostgreSQL:
    #---------------------------------------------------------------------------
    def __init__(self, agent_config, checks_logger, raw_config):
        self.agent_config = agent_config
        self.checks_logger = checks_logger
        self.raw_config = raw_config

        # get config options
        if self.raw_config.get('PostgreSQL', False):
            for key, name, required in CONFIG_PARAMS:
                self.agent_config[name] = self.raw_config['PostgreSQL'].get(key, None)
        else:
            self.checks_logger.debug(
                '%s: Postgres config section missing ([PostgreSQL]' % PLUGIN_NAME
            )
                
        # reset plugin specific params
        for param in PLUGIN_STATS:
            setattr(self, param, None)

    #---------------------------------------------------------------------------
    def run(self):
        # make sure we have the necessary config params
        for key, name, required in CONFIG_PARAMS:
            if required and not self.agent_config.get(name, False):
                self.checks_logger.debug(
                    '%s: config not complete (missing: %s) under PostgreSQL' % (
                        PLUGIN_NAME,
                        key
                    )
                )
                return False

        # plugin expects psycopg2 to be available
        try:
            import psycopg2
        except ImportError, e:
            self.checks_logger.error('%s: unable to import psycopg2' % PLUGIN_NAME)
            return False
        if not self.agent_config.get('PostgreSQLPort'):
            self.agent_config['PostgreSQLPort'] = 5432

        # connect
        try:
            db = psycopg2.connect(
                database=self.agent_config.get('PostgreSQLDatabase'),
                user=self.agent_config.get('PostgreSQLUser'),
                password=self.agent_config.get('PostgreSQLPassword'),
                port=self.agent_config.get('PostgreSQLPort')
            )
        except psycopg2.OperationalError, e:
            self.checks_logger('%s: PostgreSQL connection error: %s' % (PLUGIN_NAME, e))
            return False

        # get version
        if self.postgresVersion == None:
            try:
                cursor = db.cursor()
                cursor.execute('SELECT VERSION()')
                result = cursor.fetchone()
                self.postgresVersion = result[0].split(' ')[1]
            except psycopg2.OperationalError, e:
                self.checks_logger(
                    '%s: SQL query error when gettin version: %s' % (PLUGIN_NAME, e)
                )

        # get max connections
        try:
            cursor = db.cursor()
            cursor.execute(
                "SELECT setting AS mc FROM pg_settings WHERE name = 'max_connections'"
            )
            self.postgresMaxConnections = cursor.fetchone()[0]
        except psycopg2.OperationalError, e:
            self.checks_logger(
                '%s: SQL query error when getting max connections: %s' % (PLUGIN_NAME, e)
            )
        try:
            cursor = db.cursor()
            cursor.execute("SELECT COUNT(datid) FROM pg_database AS d LEFT JOIN pg_stat_activity AS s ON (s.datid = d.oid)")
            self.postgresCurrentConnections = cursor.fetchone()[0]
        except psycopg2.OperationalError, e:
            self.checks_logger(
                '%s: SQL query error when getting current connections: %s' % (PLUGIN_NAME, e)
            )

        # get locks
        try:
            self.postgresLocks = []
            cursor = db.cursor()
            cursor.execute("SELECT granted, mode, datname FROM pg_locks AS l JOIN pg_database d ON (d.oid = l.database)")
            for results in cursor.fetchall():
                self.postgresLocks.append(results)
        except psycopg2.OperationalError, e:
            self.checks_logger('%s: SQL query error when getting locks: %s' (PLUGIN_NAME, e))

        # get logfile info
        try:
            self.postgresLogFile = []
            cursor = db.cursor()
            cursor.execute("SELECT name, CASE WHEN length(setting)<1 THEN '?' ELSE setting END AS s FROM pg_settings WHERE name IN ('log_destination','log_directory','log_filename','redirect_stderr','syslog_facility') ORDER BY name;")
            for results in cursor.fetchall():
                self.postgresLogFile.append(results)
        except psycopg2.OperationalError, e:
            self.checks_logger(
                '%s: SQL query error when checking log file settings: %s' % (PLUGIN_NAME, e)
            )

        # return the stats
        stats = {}
        for param in PLUGIN_STATS:
            stats[param] = getattr(self, param, None)
        return stats

    
if __name__ == "__main__":
    postgres = Postgres(None, None, None)
    print postgres.run()
