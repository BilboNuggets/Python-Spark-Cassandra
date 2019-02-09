import os, sys, linecache

class GeneralHandler:
    
    def __init__(self):
        return
    
    def printException(self):
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        print 'EXCEPTION IN (LINE {} "{}"): {}'.format(lineno, line.strip(), exc_obj)
