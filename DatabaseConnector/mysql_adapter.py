import AbstractDBAdapter
import MySQLdb
import warnings

class DBAdapter(AbstractDBAdapter.AbstractDBAdapter):
    
    def __init__(self):
        # Turn MySQL warn into exceptions
        warnings.simplefilter('error', MySQLdb.Warning)
    
    
    def connect(self, opts):
        self.connection = MySQLdb.connect( opts['host'],
                                           opts['user'],
                                           opts['password'])
        return self.connection


    def close(self):
        self.connection.close()
    
    
    def select_db(self, db_name):
        return self.connection.select_db(db_name)
    
    
    def execute(self, sql):
        cursor = self.connection.cursor()
        
        try:
            cursor.execute(sql)
            self.connection.commit()
            cursor.close()
            
        except Exception, e:
            raise e
        
        finally:
            cursor.close()
        
    def select(self):
        pass
    
    def insert(self):
        pass
        
    def createTable(self, name, columns, ifNotExists = True):
        
        sql = "CREATE TABLE "
        if ifNotExists:
            sql += "IF NOT EXISTS "
            
        sql+= "`%s` (" % name
        sql+= ",".join(columns)
        sql+= ") ENGINE=InnoDB DEFAULT CHARSET=utf8"
        
        try:
            self.execute(sql)
            
            return True
        
        except MySQLdb.Warning:
            # Table already exists
            raise AbstractDBAdapter.DBAdapterError(1,'Table Already Exist')
        
        except:
            raise AbstractDBAdapter.DBAdapterError(2, 'Error creating table')