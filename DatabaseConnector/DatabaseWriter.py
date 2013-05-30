import MySQLdb
import sys
import string
import random
import re
import logging
#import warnings
import importlib

from ColoredFormatter import *

from DBAdapterExceptions import *

SQL_LOGGER_LVL = 9

class DatabaseWriter:
    """Object for communicating with a DB

    host -- [required] (str)
    user -- [required] (str)
    password -- [required] (str)
    db_name -- [required] (str)
    rebuild_db -- (bool) Overwrite existing DB
    db_adapter -- (str) Relational DB Management System
    --
    return -- DatabaseWriter object on DB success
    """

    def __init__(self, host, user, password, db_name, rebuild_db = False,
                 db_adapter = 'mysql'):
        
        # Import the right adapter
        try:
            self.dbAdapter = importlib.import_module('DatabaseConnector.mysql_adapter')
        except:
            sys.exit()
                
        # Logger
        logging.addLevelName(SQL_LOGGER_LVL, "SQL")
        logging.Logger.sql = sql_logger
        logging.setLoggerClass(ColoredLogger)
        self.logger = logging.getLogger('DatabaseWriter')

        # Regex
        self.matchAngleBrackets = re.compile(r"<(.*)>")

        # Should check and raise custom exception, but MySQL will
        # handle the problem itself!
        #self.connection = MySQLdb.connect(host, user, password)
        self.db = self.dbAdapter.DBAdapter()
        self.connection = self.db.connect({ 'host': host,
                                            'user': user,
                                            'password': password })
        
        # == Check DB presence
        #
        # Create DB if it does not exist
        if self._checkHistoryDB(db_name, rebuild_db):
            # Here you have the DB (created or already existing),
            # so connect to it!
            self.db.select_db(db_name)
        else:
            # For some reason you don't have the DB and it cannot be
            # created: exit!
            self.logger.critical('The DB doesn\'t exist and it cannot be created')
            sys.exit()
        

        # == Create Historical Service related tables
        #
        # Why these is in the __init__? This is considered
        # an atomic init operation, if one of the table is not created
        # the History Service cannot start!
        if not self._checkHistoryTables():
            self.logger.critical('Essential History tables cannot be created')
            sys.exit()
       
        
        # == Caching
        # TODO: implement circular buffering
        
        # Tables name
        # {hasCar: {
        #    table: ne23jh34hi34h,
        #    object_property: True }
        # }
        self.propertyNamesDictionary = {}
        
        # Instances ID
        # {car1: {
        #    id: 123,
        #    removed: True }
        # }
        self.instances = {}
        
        self._initNamesCaching()
            
        
        
        
    def quit(self):
        """Closes connection
        """
        self.db.close()


    def _checkHistoryDB(self, db_name, rebuild_db = False):
        """
        Check the existence of the DB and create it if doesn't exist.
        
        db_name -- (str) Name of the DB to check
        rebuild_db -- (bool) True: drop the DB and build a new one
        --
        return -- True means that a DB with the given name is available
                  at the end of the procedure. False, the DB is not 
                  available or cannot be rebuilt
        """
        
        # DB Drop
        try:
            # Delete DB, easier than dropping all tables...
            if rebuild_db:
                self.db.execute("DROP DATABASE `%s`" % db_name)
                self.logger.info('Database DROPped for rebuilding')
                
        except MySQLdb.Error, e:
            # Raised when trying to drop a DB that doesn't exist.
            # No big deal, continue the operations!
            if e[0] == 1008:
                self.logger.info("Can't DROP %s, it doesn\'t exist" % db_name)
            else:
                # Maybe the user doesn't have permission, anyway the
                # rebuild request cannot satisfied, so return false!
                self.logger.warning('Can\'t DROP DB')
                self.logger.warning(e)
                return False
        
        
        # DB Creation
        try:
            self.db.execute("CREATE DATABASE `%s`" % db_name)
            self.logger.info("Database '%s'successfully created" % db_name)
            
            return True

        except MySQLdb.Error, e:
            # Raised when trying to create an existing DB
            if e[0] == 1007:
                self.logger.info("DB '%s' already exists, not creating" % db_name)
                return True
            else:
                self.logger.warning('Cant create DB')
                self.logger.warning(e)
                return False
            
    
    def _checkHistoryTables(self):
        """
        Check the existence of essential History tables and create if not.
        Essential tables are: 'records', 'instances', 'name_dictionary'
        
        [No input params]
        --
        return -- True on success, False on fail
        """
        try:
            # Records
            cols = ('`ID` int(11) NOT NULL AUTO_INCREMENT',
                    '`Timestamp` timestamp NOT NULL',
                    'PRIMARY KEY (`ID`)')
            #if not self._createTable('Records', cols): 
            #    return False
            self.db.createTable('Records', cols)
    
            # Property Names dictionary
            cols = ('`uri` varchar(255) NOT NULL',
                    '`table_name` varchar(255) NOT NULL',
                    '`object_property` boolean NOT NULL')
            #if not self._createTable('PropertyNamesDictionary', cols): 
            #    return False
            self.db.createTable('PropertyNamesDictionary', cols)
    
            # Instances
            cols = ('`ID` int(11) NOT NULL AUTO_INCREMENT',
                   '`class` varchar(255) DEFAULT NULL',
                   '`instance` varchar(255) NOT NULL',
                   '`Removed` boolean NOT NULL',
                   'PRIMARY KEY (`ID`)')
            #if not self._createTable('Instances', cols): 
            #    return False
            self.db.createTable('Instances', cols)
            
        except DBAdapterError, e:
            self.logger.debug(e)
        
        return True
        
        
    def _initNamesCaching(self, buffer_size = 0):
        """
        Init and fill vars for caching in a circular buffer.
        Actual caching support: instances names and properties names
        
        buffer_size -- (int) Size of the circular buffer
        --
        return -- (bool) True on success
        """        
        
        try:
            cursor = self.connection.cursor()
            
            # Load existing names
            sql = "SELECT uri, table_name, object_property FROM `PropertyNamesDictionary`"
            cursor.execute(sql)
    
            row = cursor.fetchone()

            while row:
                # {haskm: {table: a34kl432lhl5, object_propery: True}}
                self.propertyNamesDictionary[row[0]] = {
                    'table': row[1],
                    'object_property': row[2] == 1}

                row = cursor.fetchone()
    
    
            # Load existing instances
            sql = "SELECT id, instance, Removed FROM `Instances`"
            cursor.execute(sql)
    
            row = cursor.fetchone()
            while row:
                # {http://arces/person_1: 0}
                self.instances[row[1]] = {'id': row[0], 'removed': row[2] == 1}
                row = cursor.fetchone()
            
            self.logger.debug('Instances cache:')
            self.logger.debug(self.instances)
            
            
            self.logger.info('Names caching: OK')
            return True
        
        except:
            self.logger.info('Names caching: Error')
            return False
        
        finally: cursor.close()


    def createInstance(self, instance, klass = None, removed = False):
        """Creates an instance in the Instances table and optionally creates
        an ad-hoc table for the class and insert the instance into it.

        instance -- (str) Full URI of the instance
        klass    -- [optional] Full URI of the class of the instance
        removed  -- (bool) True if the the instance has been deleted
        
        --
        
        return -- (int) SQL ID of the created instance, 0 on fail
        """

        klass = klass or 'NULL'
        
        instance = self._get_uri(instance) or instance
        
        cursor = self.connection.cursor()
        
        # Insert a row in the Instances table
        sql = "INSERT INTO Instances(instance, class, Removed)\
            VALUES ('%s', '%s', %d)" % (instance, klass, 1 if removed else 0)
        cursor.execute(sql)
        self.connection.commit()

        # Cache instance
        last_row_id = cursor.lastrowid
        cursor.close()

        self.instances[instance] = {'id': last_row_id, 'removed': removed}
        
        self.logger.debug('Created instance "%s" with ID %d'%(instance, last_row_id))

        return last_row_id
    
    def createPropertyTable(self, name, object_property):
        """
        Creates a table with a random name and register it in the name
        dictionary table and in cache
        
        name -- (str) The URI of the property
        object_property -- (bool)
        --
        return -- (str/bool) The random name on success, False on fail
        """
        
        # Supports both name with or without <>
        uri = self._get_uri(name) or name
        
        # TODO: Enhance reliability, commit after executing
        # the property table creation
        random_table_name = self._registerTableName(uri, object_property)

        try:
            #cursor = self.connection.cursor()            
            cols = ( '`SubjectID` int(11) NOT NULL',
                     '`Object` int(11) NOT NULL',
                     '`RecordID` int(11) NOT NULL',
                     '`Removed` boolean NOT NULL' )
            self.db.createTable(random_table_name, cols, False)
        
        except DBAdapterError, e:
            # 1) If here trying to create a table with the same name
            #    the random name algorithm generated two identical
            #    string, but this situation should be handled in
            #    _registerTableName(), so shouldn't occur
            # 2) Generic MySQL error, the table cannot be created for
            #    some reason
            #
            # Its correcto to return False!
            self.logger.warning(e)
            return False
        
        self.logger.debug('Created property "%s"' % uri)
        return random_table_name


    def removeTriples(self,triples):
        self._writeTriples(triples, removed = True)

    def addTriples(self, triples):
        self._writeTriples(triples, removed = False)
    
    
    def _writeTriples(self, triples, removed):
        """Execute a write action on the DB. Dont call this function directly,
        use the higher level specific functions
        
        triples -- (list) List of triples each with 4 elements (subj, pred, obj,
            obj_type)
        removed -- (bool) True if the triple must flagged as removed, False
            otherwise    
        --
        return -- (int) The number of NOT written triples, 0 means fine!
        """
        
        if removed == None: return False
        
        # Triple not written due to errors
        not_written = 0
        
        for triple in triples:
            # Unpack triple, _t is True for uri or False for literal
            _s, _p, _o, _t = triple
            _s = str(_s); _p = str(_p); _o = str(_o)

            # Get subject ID
            subject = self.getInstanceID(_s)
            
            # If instance exists
            if subject: subject = subject[0]
            
            # If subj doesn't exist create it
            else: subject = self.createInstance(_s, None, False)
            
            
            # If Object is URI
            if _t:
                # Get its ID
                objekt = self.getInstanceID(_o)
                if objekt: objekt = objekt[0]
                
                else: objekt = self.createInstance(_o, None, False)
            
            else:
                objekt = '"'+str(_o)+'"'
            
            # Fetch property table name (a random string) from cache
            property_table_name = self.getPropertyTableName(_p)
            
            # Create table if it doesn't exist
            if property_table_name:
                if not (property_table_name[1] == _t):
                    self.logger.warning('Triple "%s" not written: obj type not correct' %
                        str(triple) )
                    not_written += 1
                    continue
                
                property_table_name = property_table_name[0]
            else:
                property_table_name = self.createPropertyTable(_p, _t)
                
                
            if not(subject) or not(property_table_name) or not(objekt):
                self.logger.warning('Cant write triple "%s"->"%s"' %
                        ( str(triple), str(subject, property_table_name, object) ) )
                not_written += 1
                continue
            
            # Register current timestamp on the Records table
            # None argument defaults to now!
            record_id = self._registerTimeRecord(None)

            sql = "INSERT INTO `%s`(SubjectID, Object, RecordID, Removed)\
               VALUES (%s, %s, %s, %d)" % (property_table_name, 
                                       subject, objekt, record_id,
                                       1 if removed else 0)
            self.logger.sql(sql)
            self.db.execute(sql)
            
        return not_written


    def readTriples(self, vars, triples):
        """
SELECT r.ID, r.Timestamp, hc.SubjectID as user, hc.ObjectID as car, NULL as car, NULL as km FROM `Records` AS r
JOIN `BuIjXTaMhFtIjxEkJBPeNbgl2Lhwta6f` AS hc ON r.ID = hc.RecordID

UNION

SELECT r.ID, r.Timestamp, NULL as user, NULL as car, hk.SubjectID as car, hk.Object as km FROM `Records` AS r
JOIN `MRGlLBdFTyMQDb3hdNaVhTc9FKVYv2XG` AS hk ON r.ID = hk.RecordID
        """
        
        self.logger.warning('Dont use this function')
        
        sql_select = "SELECT "
        sql_from   = " FROM "

        sql_select_vars = []
        sql_where  = []

        if len(triples) == 1:
            triple = triples[0]
            # Unpack triple
            _s, _p, _o = triple
            # Get SQL table name (random string) from the property URI
            table_name = self.propertyNamesDictionary[self._get_uri(triple[1])]['table']
            sql_from += " `%s` AS tn " % table_name

            # Subject
            if triple[0].__class__ == str:
                # It is an URI, not a var (cant be literal)
                subject = self.getInstanceID( self._get_uri(triple[0]) )
                sql_where.append("tn.SubjectID='%s'" % subject)
            else:
                # Subject is a var
                if triple[0].name in vars:
                    sql_select_vars.append('SubjectID')


            # Object
            if _o.__class__.__name__ in ('str' ,'SparqlLiteral'):
                # It is an URI or literal, not a var
                if _o.__class__.__name__ == 'SparqlLiteral': _o = _o.value
                uri = self._get_uri(_o)
                # Check if URI or literal
                objekt = self.getInstanceID(uri) if uri else _o
                sql_where.append("tn.Object='%s'" % objekt)
            else:
                # Object is a var
                if triple[2].name in vars:
                    sql_select_vars.append('Object')

            if len(sql_select_vars):
                sql_select += ','.join(sql_select_vars)
            else:
                sql_select += '*'

            if len(sql_where):
                sql_from   += 'WHERE ' + ' AND '.join(sql_where)

            #cursor.execute(sql_select + sql_join)
            return sql_select + sql_from

        else:
            for t in triples:
                pass


    # Instances caching - {http://arces/person_1: 0}
    # Why not stored in array by id? because ids may not be contigous and
    # you can't have 'jumps' in array indexes
    def _getInstanceURI(self, instance_id):
        self.logger.warning('DEPRECATED: _getInstanceURI')
        return self.getInstanceURI(self, instance_id)
        
    def getInstanceURI(self, instance_id):

        # TODO: implement circular buffer cache for the last N used
        for uri in self.instances:
            _i = self.instances[uri]
            
            if _i['id'] == instance_id: 
                return uri, _i['removed']
            
        # Get from DB table
        try:
            cursor = self.connection.cursor()
            sql = "SELECT ID, instance, Removed FROM `Instances`\
            WHERE ID = '%s' LIMIT 1" % instance_id
            cursor.execute(sql)
            
            row = cursor.fetchone()
            if row and len(row) > 0:
                self.instances[row[1]] = {'id': row[0], 'removed': row[2]}
                return row[1], row[2]
        
        except MySQLdb.Error, e:
            # If the property doesn't exist result is None, no one raise
            # exceptions!
            self.logger.debug(e)
            return False
            
        finally: cursor.close()
        
        self.logger.debug('Instance with ID "%s" doesn\'t exist', instance_id)
        return False
    

    def _getInstanceID(self, instance_uri, with_angular = True):
        self.logger.warning('DEPRECATED: _getInstanceID')
        return self.getInstanceID(self, instance_uri, with_angular = False)

    def getInstanceID(self, instance_uri, with_angular = True):
        """Get the ID of the instance as stored in the DB table 
        """
        
        # Accepts URIs with or without angle parenthesis <uri://here>
        instance_uri = self._get_uri(instance_uri) or instance_uri
        
        # May use 'if instance_uri in self.instances' and then return
        # it but you must search twice!
        try:
            _i = self.instances[instance_uri]
            return _i['id'], _i['removed']
        except KeyError:
            self.logger.debug('Instance %s not in cache',instance_uri)
            
            
        # Get from DB table
        try:
            cursor = self.connection.cursor()
            sql = "SELECT ID, instance, Removed FROM `Instances`\
            WHERE instance = '%s' LIMIT 1" % instance_uri
            cursor.execute(sql)
            
            row = cursor.fetchone()
            self.logger.sql(row)
            if row and len(row) > 0:
                self.instances[row[1]] = {'id': row[0], 'removed': row[2]}
                return row[0], row[2]
        
        except MySQLdb.Error, e:
            # If the property doesn't exist result is None, no one raise
            # exceptions!
            self.logger.debug(e)
            return False
            
        finally:
            if cursor: cursor.close()
            
        self.logger.debug('Instance with URI "%s" doesn\'t exist', instance_uri)
        self.logger.sql(sql)
        return False
        

    def _getPropertyTableName(self, property, with_angular = True):
        self.logger.warning('DEPRECATED: _getPropertyTableName')
        return self.getPropertyTableName(property, with_angular)
        
    def getPropertyTableName(self, property, with_angular = True):
        """Get the name of the table used for storing the property. If the name
        is not in cache, it is fetched from the name dictionary table. If
        the property table doesn't exist it is not created and return False.
        
        property -- (str) name of the property
        --
        return -- (list) [table, property_type] or False on failure
        """
        
        #if with_angular: property = self._get_uri(property)
        property = self._get_uri(property) or property
        
        
        # Get from cache
        try:
            prop = self.propertyNamesDictionary[property]
            return prop['table'], prop['object_property']
        except KeyError:
            self.logger.debug('Property name "%s" not in cache' % property)
        
        # Get from DB table
        try:
            cursor = self.connection.cursor()
            sql = "SELECT uri, table_name, object_property FROM `PropertyNamesDictionary`\
            WHERE uri = '%s' LIMIT 1" % property
            cursor.execute(sql)
            
            row = cursor.fetchone()
            if row and len(row) > 0:
                self.propertyNamesDictionary[row[0]] = {
                    'table': row[1],
                    'object_property': row[2] == 1}
                return row[1], (row[2] == 1)
        
        except MySQLdb.Error, e:
            # If the property doesn't exist result is None, no one raise
            # exceptions!
            self.logger.debug(e)
            return False
            
        finally: cursor.close()
            
        # The table doesn't exist, create it
        #
        # createPropertyTable returns the table name on success or false
        # on fail
        self.logger.debug('Property table %s doesn\'t exist' % property)
        return False
        #return self.createPropertyTable(property, op)
        

    def _registerTableName(self, name, object_property):
        """Register a random string of 32 ascii alpha-numeric chars the first
        of which is always a letter in the naming-dictionary and assciates
        it to a given name

        name -- Friendly name to register in the naming-dictionary
        object_property -- Boolean
        ---
        return -- Random string of 32 chars (1st always a letter) associated to the
        name
        """

        random_name = self._random_id()
        op = 1 if object_property else 0

        cursor = self.connection.cursor()
        sql = "INSERT INTO PropertyNamesDictionary(uri, table_name, object_property)\
           VALUES ('%s', '%s', '%d')" % (name, random_name, op)
        cursor.execute(sql)
        self.connection.commit()
        cursor.close()

        # Cache name
        self.propertyNamesDictionary[name] = {
            'table': random_name,
            'object_property': object_property}

        return random_name

    def _registerTimeRecord(self, timestamp = None):
        """
        """

        timestamp = timestamp or 'NULL'

        # No ' around %s otherwise 'NULL' becomes timestamp 0
        cursor = self.connection.cursor()
        sql = "INSERT INTO Records(Timestamp)\
           VALUES (%s)" % (timestamp)
        cursor.execute(sql)
        self.connection.commit()
        
        lastrowid = cursor.lastrowid
        
        cursor.close()

        return lastrowid



    def _get_uri(self, string):
        """ Extracts content between angle brackets <urihere>
        """
        m = self.matchAngleBrackets.match(string)
        return (False if (m == None) else m.group(1))

    def _random_id(self, size=32, chars=string.ascii_letters+string.digits):
        """Return a random string of alphanumeric chars, where the first
        is always a letter

        size -- Length of the string, default to 32. Caution! Many DB allows
                32 chars as maximum length for the name
        chars -- String of the allowed chars whence to pick the chars for the
                 random string. Default is string.ascii_letters+string.digits
        ---
        return -- string
        """
        return (random.choice(string.ascii_letters)+ # First char is a letter
            ''.join(random.choice(chars) for x in range(size-1)))
        
def sql_logger(self, message, *args, **kws):
    self._log(SQL_LOGGER_LVL, message, args, **kws)