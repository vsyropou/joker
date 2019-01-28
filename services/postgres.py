import abc
import os
import json
import asyncio
import asyncpg

from utilities.general import info

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

        # format records
        self.records_formater = lambda r: [v for v in r.values()]

        info('Instantiated db client to: "%s" database @%s.'%(self.database,self.host))

    def _check_password(self):
                
        classname = self.__class__.__name__
        prefix = classname.split('PostgresDatabaseService')[0]

        self._credentials_path = os.path.join('/tmp/%s_postgress_crd.json'%prefix)
        if not os.path.exists(self._credentials_path):
            msg = 'Provide password for database "%s" (hostname: %s): \n> '%(prefix,self.host)
            pwd = input(msg)
            print()
            
            with open(self._credentials_path, 'w+') as fp:
                json.dump(pwd,fp)

    def query(self, qry):

        async def run():

            with open(self._credentials_path,'r') as fp:
                pwd = json.load(fp)
        
            conn = await asyncpg.connect(user = self.user,
                                         password = pwd,
                                         database = self.database,
                                         host = self.host)

            records = await conn.fetch(qry)

            await conn.close()
            
            return records

        # excecute thread
        query_result = asyncio.get_event_loop().run_until_complete(run())

        return list(map(self.records_formater, query_result))


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
