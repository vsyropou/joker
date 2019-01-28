
import sys
import requests

from services.general import MessageService

# print helpers
info = MessageService.info
error = MessageService.error
warn = MessageService.warn
debug = MessageService.debug


class Progress():

    _counter = 0

    _annimations_ = ['-', '\\','|','/']    

    def __init__(self, total, name=None):

        self._name = name
        
        self._total = total
        
        self._counter = Progress._counter

        Progress._total = self._total
        Progress._name  = self._name
        
    @classmethod
    def progress(cls, count):

        if cls._counter == 0:
            msg  = 'Displaying progress'
            msg += ' of "%s":'%cls._name if cls._name else ''
            msg += ' ( Make coffee and be patient :-P ):'
            info(msg)
            
        annimation_idx = cls._counter % len(Progress._annimations_)
        annimation_icn = Progress._annimations_[annimation_idx]
        
        bar_len = 45
        filled_len = int(round(bar_len * count / float(cls._total)))

        percents = round(100.0 * count / float(cls._total), 1)
        bar = '=' * filled_len + '-' * (bar_len - filled_len)

        sys.stdout.flush()
        
        sys.stdout.write('[%s] %s%s ... [%s]\r' % (bar, percents, '%', annimation_icn))

        sys.stdout.flush()
        
        cls._counter += 1


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

