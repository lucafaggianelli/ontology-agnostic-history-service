import threading
import time

from ColoredFormatter import *
import logging

class HistoryDecanter(threading.Thread):
    
    def __init__(self, db):
        # Threading init
        threading.Thread.__init__(self)
        
        # Logger
        logging.setLoggerClass(ColoredLogger)
        self.logger = logging.getLogger('Decanter')
        
        # Params
        self.period = 5
        
        # Decanter vars
        self.db = db
        self.stop = 0
        self.decanter = []
        
        
    def run(self):
        # The decanter runs till it is stopped, but then
        # it finish to empty the buffer
        while not self.stop or len(self.decanter):
            
            # Wakes up every 'self.period' seconds
            time.sleep(self.period)
            
            # Copy the buffer, empty it and write it
            toWrite = self.decanter
            self.decanter = []
            self.db.writeTriples(toWrite)
            
            self.logger.debug(toWrite)
        
            
    def kill(self):
        self.stop = 1
        
        
    def addTriples(self, triples):
        
        # Cannot add triples when it is stopped
        if self.stop: return 0
        
        # Remove triples already present in the decanter
        for i, _t in enumerate(triples):
            if _t in self.decanter:
                triples.pop(i)
        
        self.decanter.extend(triples)
        
        return len(triples)