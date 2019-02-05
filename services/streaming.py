
from utilities.general import info

class DataStreamerSql():
    
    def __init__(self, *args, **kwargs):

        bdbcknd = args[0]
        query = args[1]

        step = kwargs.pop('step', 1)
        rec_frmt = kwargs.pop('records_format', 'dict')
        
        # TODO: check all args
        cursor = bdbcknd.cursor()
        cursor.execute(query)

        self.nrows = cursor.rowcount
        self.batch_size = step
        
        self.column_names = [d.name for d in  cursor.description]

        # format
        formater = getattr(self, '%s_formater'%rec_frmt) 
        records  = lambda : formater(cursor.fetchmany(step))
        
        self._generator = (records() for _ in range(0,self.nrows,step))

    def __enter__(self):
        info('Executing stream')
        return self._generator

    def __exit__(self, *args):
        info('Processed stream')

    def __call__(self):
        return self._generator
    
    def dict_formater(self, recs):
        return [{nam:val for nam, val in zip(self.column_names,rec)} for rec in recs]

    def list_formater(self, recs):
        return recs
