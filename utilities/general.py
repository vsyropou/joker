
import sys
import json
import requests

from services.general import MessageService

# print helpers
info = MessageService.info
error = MessageService.error
warn = MessageService.warn
debug = MessageService.debug


class Progress():

    _annimations_ = ['-', '\\','|','/']    

    def __init__(self, total, name=None):

        self._name = name
        
        self._total = total
        
        self._counter = 0

    def __enter__(self):
        msg  = 'Displaying progress'
        msg += ' of "%s":'%self._name if self._name else ''
        msg += ' ( Make coffee and be patient :-P ):'
        info(msg)

        return self
        
    def __call__(self, jump=1):

        annimation_idx = self._counter % len(self._annimations_)
        annimation_icn = self._annimations_[annimation_idx]
        
        bar_len = 45
        filled_len = int(round(bar_len * self._counter / float(self._total)))

        percents = round(100.0 * self._counter / float(self._total), 1)
        bar = '=' * filled_len + '-' * (bar_len - filled_len)

        sys.stdout.flush()
        
        sys.stdout.write('[%s] %s%s ... [%s]\r' % (bar, percents, '%', annimation_icn))

        sys.stdout.flush()
        
        self._counter += jump

    def __exit__(self, *args):
        self._counter = self._total
        self()
        self._counter = 0
        print()


def read_json(path):
    with open(path,'r') as fp:
        return json.load(fp)

# fun stuff
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

