
import sys
import requests

from services.general import MessageService

# print helpers
info = MessageService.info
error = MessageService.error
warn = MessageService.warn
debug = MessageService.debug


class Progress():
    #TODO:  make this a context manager
    _annimations_ = ['-', '\\','|','/']    

    def __init__(self, total, name=None):
        # TODO: should __enter__()
        self._name = name
        
        self._total = total
        
        self._counter = 0

    def reset(self, total=None):
        #TODO: should be exit(), plus print one line
        self._total = self._total if not total else total
        self._counter = 0


    def progress(self, jump=1):
        # TODO: this should be returned by the with statement
        if self._counter == 0:
            msg  = 'Displaying progress'
            msg += ' of "%s":'%self._name if self._name else ''
            msg += ' ( Make coffee and be patient :-P ):'
            info(msg)
            
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
        if self._counter>=self._total:
            print()


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

