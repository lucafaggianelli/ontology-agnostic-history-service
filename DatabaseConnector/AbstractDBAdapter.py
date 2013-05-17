from DBAdapterExceptions import *

class AbstractDBAdapter():

    def __init__(self):
        pass
    
    def connect(self):
        pass
    
    def close(self):
        pass
    
    def select_db(self):
        pass
    
    def execute(self):
        pass