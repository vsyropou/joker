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

        # look for password
        self._check_password()

        debug('Instantiated db client to: "%s" database @%s.'%(self.database,self.host))


    def _check_password(self):

        prefix = self.__class__.__name__.split('PostgresDatabaseService')[0]

        credentials_path = os.path.join('/tmp/%s_postgress_crd.json'%prefix)

        # ask for pwd, if needed
        if not os.path.exists(credentials_path):
            msg = 'Provide password for database "%s" (hostname: %s): \n> '%(prefix,self.host)
            pwd = input(msg)
            print()
            # write
            with open(credentials_path, 'w+') as fp:
                json.dump(pwd,fp)

        # test connection
        try:
            self.conn = psycopg.connect(user = self.user,
                                        password = read_json(credentials_path),
                                        database = self.database,
                                        host = self.host)

        except Exception as err:
            error('Error while connecting to "%s@%s" as %s'%(self.database,self.host,self.user))
            error(err)
            raise

    def cursor(self):
        return  self.conn.cursor()

    def execute(self, qry):

        try:
            cursor = self.cursor()
            cursor.execute(qry)
        except Exception as err:
            error('Caught runtime postgres exception')
            print(err)
            raise

        return cursor.fetchall()

    def execute_insert(self, qry):

        cursor = self.cursor()
        try:
            cursor.execute(qry)
            self.conn.commit()

        except Exception as err:

            self.conn.rollback()
            
            if err.pgcode == '23505':
                warn('Primary key vioaltion. Rolled back query: %s'%qry)
            else:
                error('Runtime exception caught, when: %s'%qry)
                print(err,err.pgcode)
                raise


        cursor.close()


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
