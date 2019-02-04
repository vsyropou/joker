import abc
import os
import json
import asyncio
import asyncpg
from utilities.general import warn, info, error, debug

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
        run = asyncio.get_event_loop().run_until_complete
        try:
            conn = run(asyncpg.connect(user = self.user,
                                       password = json.load(open(credentials_path,'r')),
                                       database = self.database,
                                       host = self.host))
        except Exception as err:
            error('Error while connecting to "%s@%s" as %s'%(self.database,self.host,self.user))
            error(err)
            raise

    def query(self, qry):

        prefix = self.__class__.__name__.split('PostgresDatabaseService')[0]
        credentials_path = os.path.join('/tmp/%s_postgress_crd.json'%prefix)
        pwd = json.load(open(credentials_path,'r')),

        async def run_query(qry): # keep it nested for security
            conn = await asyncpg.connect(user = self.user,
                                         password = pwd,
                                         database = self.database,
                                         host = self.host)

            records = await conn.fetch(qry)

            await conn.close()
            
            return records

        try: # get event loop
            asyncio.get_event_loop()
        except RuntimeError as err:
            warn('Caught eception: %s'%err)
            asyncio.set_event_loop(asyncio.new_event_loop())
            info('Creating event loop')

        # run event loop
        query_result = asyncio.get_event_loop().run_until_complete(run_query(qry))

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
