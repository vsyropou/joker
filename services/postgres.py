import abc
import os
import json

import psycopg2 as psycopg

from utilities.general import warn, info, error, debug
from utilities.general import read_json

__all__ = ['PostgresReaderService', 'PostgresWriterService']


class AbsPostgressService(abc.ABC):

    @property
    @abc.abstractmethod
    def host(self):
        pass

    @property
    @abc.abstractmethod
    def user(self):
        pass

    @property
    @abc.abstractmethod
    def database(self):
        pass


class BasePostgressService(AbsPostgressService):

    def __init__(self):

        # check tmp dir path exists
        assert os.path.exists('/tmp'), \
            'Path "%s" does not exist. Try specifieng path correctly or set the '\
            'global property "tmp_directory_path" accordingly'

        self._class_prefix = self.__class__.__name__.split('PostgresDatabaseService')[0]

        # chcek password and connection
        self._check_connection()

        debug('Instantiated db client to: "%s" database @%s.'%(self.database,self.host))

    def _check_connection(self):
        
        credentials_path = os.path.join('/tmp/%s_postgress_crd.json'%self._class_prefix)

        # ensure password
        if not os.path.exists(credentials_path):
            msg = 'Provide password for database "%s" (hostname: %s): \n> '%(self._class_prefix,self.host)
            pwd = input(msg)
            print()
            # write out pwd
            with open(credentials_path, 'w+') as fp:
                json.dump(pwd,fp)

        # test connection
        self._connect(read_json(credentials_path)).close()


    def _connect(self, pwd):
        try:
            conn = psycopg.connect(user = self.user,
                                   password = pwd,
                                   database = self.database,
                                   host = self.host)
        except Exception as err:
            error('Error while connecting to "%s@%s" as %s'%(self.database,self.host,self.user))
            error(err)
            raise        

        return conn

    def cursor(self):

        credentials_path = os.path.join('/tmp/%s_postgress_crd.json'%self._class_prefix)
        pwd = read_json(credentials_path)

        conn = self._connect(pwd)

        return conn.cursor()

    def execute(self, qry):

        try:
            cursor = self.cursor()
            cursor.execute(qry)
        except Exception as err:
            conn.rollback()
            raise

        results = cursor.fetchall()

        cursor.connection.close()
        
        return results

    def execute_insert(self, qry):
        # TODO: run this in a seperate thread, maybe
        cursor = self.cursor()
        conn = cursor.connection

        try:
            cursor.execute(qry)
            conn.commit()
        except Exception as err:
            conn.rollback()
            raise

        cursor.close()
        conn.close()

        return True

class PostgresReaderService(BasePostgressService):

    @property
    def host(self):
        return 'localhost'

    @property
    def user(self):
        return 'postgres'

    @property
    def database(self):
        return 'postgres'


class PostgresWriterService(BasePostgressService):

    @property
    def host(self):
        return 'localhost'

    @property
    def user(self):
        return 'postgres'

    @property
    def database(self):
        return 'postgres'
