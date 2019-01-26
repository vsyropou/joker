
import requests
import importlib

from services.general import MessageService

# print helpers
info = MessageService.info
error = MessageService.error
warn = MessageService.warn
debug = MessageService.debug

# funs stuff
class ChunkNorisJoke():
    
    def __enter__(self):
        return self

    def __exit__(self,*args):

        self.joke()

        if self._joke:
            print('\n %s\n'%self._joke)

    def joke(self):

        try:
            rsp = requests.get('https://api.chucknorris.io/jokes/random')
        except Exception:
            rsp = 'Sorry no jokes today....'

        try:
            self._joke = rsp.json()['value']
        except:
            self._joke = None

