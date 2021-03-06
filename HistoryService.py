from smart_m3.RDFTransactionList import *
from Utility.Ontology import *
from smart_m3.m3_kp import *
from smart_m3.m3_kp_api import *

import xml.etree.ElementTree as ET

import sys
import string
import random
import time
import getopt
import re

import logging
from ColoredFormatter import *

from HistoryDecanter import HistoryDecanter
#from HistoryHandlers import *

from DatabaseConnector import DatabaseWriter
#from OntologyAnalyzer import *

import fyzz

# REGEX
matchAngleBrackets = re.compile(r"<(.*)>")

def main(argv):
    
    _set_commandline_options(argv)
    
    # SPARQL Subscriptions of the history request
    global sparqlSubscriptions
    sparqlSubscriptions = []
    
    # Start the daemon
    historyService = HistoryService()
    
    # TODO: DAVVERO GREZZO!!!!
    try:
        while(True):
            time.sleep(10)
            
    except KeyboardInterrupt:
        historyService.quit()
        sys.exit()


class HistoryService():
    """HistoryService is the main class of the History Service Daemon. It is
    suggested to launch the script as it is with the proper command line
    options. Being a background process, the module doesn't expose any 
    interface.
    
    **Command line options**
        * ``-h``, ``--help`` : Show this help page
        * ``-r``, ``--rebuild-db`` : Drop the history DB and reinitialize the DB
            and all the tables
        * ``-l``, ``--log`` : Set log level
    """
    
    def __init__(self):
        
        # Init DB access
        database = 'history'
        global DB
        DB = DatabaseWriter.DatabaseWriter('localhost',
                                           'root',
                                           'luca123',
                                           database,
                                           False)
        
        # Init Smart M3 API
        global M3
        M3 = m3_kp_api(False, 'localhost')#'192.168.1.104')
        
        
        # Decanter
        global Decanter
        Decanter = HistoryDecanter(DB)
        Decanter.start()
        
        
        
        # Subscribe to history requests
        self.historyRequestSubscription = M3.load_subscribe_RDF([
            Triple( URI(HISTORY),
                    URI(HAS_HISTORY_INPUT), 
                    None) ],
            HistoryRequestHandler() )
        
        logger.info('Existing requests:')
        logger.info(M3.result_RDF_first_sub)
        
        # Simulate a notification for the request already present in the SIB
        if len(M3.result_RDF_first_sub):
            HistoryRequestHandler().handle(M3.result_RDF_first_sub, [])
        else:
            logger.info('No History requests')

        
        # Subscribe to read request
        self.historyReadSubscription = M3.load_subscribe_RDF([
            Triple(
                URI(HISTORY),
                URI(HAS_HISTORY_READ),
                None ) ],
            HistoryReadRequestHandler())
        
        if len(M3.result_RDF_first_sub):
            HistoryReadRequestHandler().handle(M3.result_RDF_first_sub, [])
        else:
            logger.info('No read requests')


    def quit(self):
        
        # Close subscription to History Requests
        self.historyRequestSubscription.close()
        self.historyReadSubscription.close()
        
        # Close subscriptions to SPARQL queries of the history requests
        global sparqlSubscriptions
        for sub in sparqlSubscriptions:
            sub.close()

        # Close the decanter
        Decanter.kill()

        M3.leave()


class HistoryRequestHandler():
    """Req handler
    """
    
    def handle(self,added, removed):
        """M3 handler
        """
        
        # May arrive more requests at the same time!
        for index in added:
            logger.info("New request:")
            logger.info(index)
            
            # Query to obtain the sparql query to subscribe to
            #query = theNode.CreateQueryTransaction(theSmartSpace)
            q = "select ?sparql where {<%s> <%s> ?sparql .}" %(index[2], HAS_SPARQL)
            M3.load_query_sparql(q)
            
            if len(M3.result_sparql_query):
                sparqlToSubscribeTo = str(M3.result_sparql_query[0][0][2])
                
                # If SPARQL is not good remove the request and continue the loop
                parsed = fyzz.parse(sparqlToSubscribeTo)
    
                # Subscribe to SPARQL query issued by the history request
                #subscription = theNode.CreateSubscribeTransaction(theSmartSpace)
                subscription = M3.load_subscribe_sparql(sparqlToSubscribeTo,
                                            HistorySparqlHandler(parsed.where))
                #results = subscription.subscribe_sparql(sparqlToSubscribeTo, 
                #                              HistorySparqlHandler(parsed.where))
                logger.info("Subscribed to: %s" % sparqlToSubscribeTo)
                
                # Serve the history request with existing data, otherwise when 
                # the read request gets an error (table not found)
                HistorySparqlHandler(parsed.where).handle(M3.result_sparql_first_sub,
                                                          [])
                
                # Track all the subscriptions in an array
                global sparqlSubscriptions
                sparqlSubscriptions.append(subscription)
            else:
                logger.warning('Request has no sparql query to subscribe to')

        for index in removed:
            logger.info("Removed record:")
            logger.info(index)


class HistorySparqlHandler():
    """Handler for SPARQL notification of history request
    """
    
    def __init__(self, templates):
        
        global DB
        self.db = DB
        
        # Should write to decanter and never directly to DB
        global Decanter
        self.decanter = Decanter
        
        self.templates = templates
    
    def handle(self, added, removed):
        """
        """
        
        # The order here is of PARAMOUNT importance! You need to remove prior
        # of inserting new triples!!!!
        if removed and removed != []:
            to_remove = self.build_triples_to_remove(removed)
            logger.debug(to_remove)
            #self.db.removeTriples(to_remove)
            self.decanter.addTriples(to_remove)
        
        if added and added != []:            
            to_add = self.build_triples_to_add(added)
            logger.debug(to_add)
            #self.db.addTriples(to_add)
            self.decanter.addTriples(to_add)
    

    def build_triples_to_remove(self, removed_vars):
        
        vars = self.prettify_sparql_result(removed_vars)
        
        # Sparql query translated in RDF query, where all triples are in OR
        # chain
        rdf_query = self.sparql2rdf(self.templates, vars)
        
        # RDF query to understand what has been really deleted
        qTransaction = theNode.CreateQueryTransaction(theSmartSpace)            
        result = qTransaction.rdf_query(rdf_query)
        theNode.CloseQueryTransaction(qTransaction)
        
        for triple in result:
            # This should wrap the for: exception here is a bug!
            try:
                i = rdf_query.index(triple)
                rdf_query.pop(i)
                
            except ValueError:
                logger.warning('Triple "%s" in result but not in query, strange!'%
                               str(triple))
        
        triples_to_remove = []
        for t in rdf_query:
            _uri = t[2].__class__.__name__ == 'URI'
            t = map(str, t)
            #t.append(_uri)
            t.extend([_uri, True])
            
            triples_to_remove.append(t)
        
        return triples_to_remove
        
        
    def build_triples_to_add(self, updated):
        
        for new_vars in updated:
            # new_vars =
            #[[u's', u'uri', u'http://mml.arces.unibo.it/Ontology.owl#Something_43'],
            # [u'p', u'uri', u'http://mml.arces.unibo.it/Ontology.owl#HasValue'],
            # [u'o', u'literal', u'11']]
            #
            # self.templates =
            #[(SparqlVar(car),('','a'),'<http://arces.it/Car>')]
            # literals are: SparqlLiteral('87')

            triples_to_write = [] # Init triple to write

            # Refactor new_vars as {car: <car1>, km: '129'} for better hadling
            vars = {}
            for new_var in new_vars:
                vars[new_var[0]] = {'uri': (new_var[1]=='uri')}
                vars[new_var[0]]['name'] = (new_var[2]  if new_var[1] == 'literal'
                                                        else '<'+new_var[2]+'>')

            for template in self.templates:
                triple_to_write = None
                    
                for i, elem in enumerate(template):                    
                    if ((elem.__class__.__name__ == 'SparqlVar') and 
                        (elem.name in vars)):
                        
                        # Copy the template only the first time, otherwise
                        # the second var in a triple would overwrite the first
                        triple_to_write = triple_to_write or (list(template)+[None])
                        
                        # Replace ?var with actual value
                        triple_to_write[i] = vars[elem.name]['name']
                        triple_to_write[3] = vars[elem.name]['uri']
                
                # Append to the write queue only if template contains the right
                # variable
                if triple_to_write:
                    #type = template[2].__class__.__name__ == 'SparqlVar'
                    
                    # The triple must be added, not removed
                    triple_to_write.append(False)
                    
                    triples_to_write.append(triple_to_write)
                                    
            # Prefixed syntax for uri is represented as ('prefix','id')
            # but we need 'prefix:id'
            for i, elem in enumerate(triple_to_write):
                if elem.__class__ == tuple:
                    triple_to_write[i] = ':'.join(elem)
            
            return triples_to_write
    
    
    def sparql2rdf(self, triples, vars):
        """Convert a SPARQL query in a RDF query, where the triples are chained
        with the OR operator instead of the AND (dot) SPARQL operator
        
        triples -- (list)
        vars    -- (dict)
        """
        
        rdf = []
        for triple in triples:
            if not len(triple) == 3:
                logger.warning('Found a triple of length: %d. Can\'t convert!'%
                               len(triple))
                return False
            
            _t = []
            for element in triple:
                if element.__class__.__name__ == 'SparqlVar':
                    _v = vars[element.name]
                    _e = URI(_getURI(_v['name'])) if _v['uri'] else Literal(_v['name'])
                
                elif element.__class__.__name__ == 'SparqlLiteral':
                    _e = Literal(element.value)
                
                elif element.__class__ == tuple:
                    _e = URI(':'.join(element))
                
                elif element.__class__ == str:
                    _e = URI( _getURI(element) )
                
                _t.append(_e)
            rdf.append(_t)
        
        return rdf
            
            
    def prettify_sparql_result(self, sparql_result):
        
        vars = {}
        for changed_vars in sparql_result:
            for changed_var in changed_vars:
                vars[changed_var[0]] = {'uri': (changed_var[1]=='uri')}
                vars[changed_var[0]]['name'] = (changed_var[2]  if changed_var[1] == 'literal'
                                                                else '<'+changed_var[2]+'>')
        
        return vars


class HistoryReadRequestHandler():
    """Read handler
    Test command in python prompt:
    from HistoryService import *; r = HistoryReadRequestHandler(); logger.setLevel(4); r.readHistoryData(r.test)
    """

    def __init__(self):
        
        global DB
        self.db = DB
        
        self.test = """
            SELECT ?person ?car ?km ?tire ?tireTread WHERE {
                ?person <http://rdf.tesladocet.com/ns/person-car.owl#HasCar>   ?car .
                ?car    <http://rdf.tesladocet.com/ns/person-car.owl#HasKm> ?km .
                ?car    <http://rdf.tesladocet.com/ns/person-car.owl#HasTire> ?tire .
                ?tire   <http://rdf.tesladocet.com/ns/person-car.owl#HasTireTread> ?tireTread
            }"""
    
    def handle(self, added, removed):
        """M3 Handler
        """
        for read_request in added:
            
            read = theNode.CreateQueryTransaction(theSmartSpace)
            result = read.rdf_query([
                Triple( URI(read_request[2]),
                        URI(HAS_SPARQL),
                        None ) ])
            theNode.CloseQueryTransaction(read)
            
            logger.debug(result)
            
            return self.readHistoryData(str(result[0][2]), read_request[2])
        
    
    def readHistoryData(self, sparql, read_request_uri):
        """Make a SPARQL query in a SQL DB
        """
        
        # Parse SPARQL query
        # parsed.selected => selected vars
        # parsed.variables => all vars
        parsed = fyzz.parse(sparql)
        
        # Incoming and outgoing connection to and from a node
        # {'car': [(user1, 'hasCar', None), # => Incoming
        #          (None, 'hasTire', tire10)] } # => Outgoing
        adjacency_list = {}
        
        # Select clause is given by selected vars (parsed.selected)
        # NULL AS car, NULL AS tire
        select_clause_sql = []
        
        # Contains the selected vars prepared for being injected into the SQL
        # select clause
        selected_vars_sql = []
        for var in parsed.selected:
            selected_vars_sql.append({'name': var.name, 'uri': None})
        
        # used for joining to property table, use array to save some IF
        join_on = ('SubjectID', None, 'Object')
        
        # Creation of the adjacencies list
        # ================================
        for triple in parsed.where:
            _s ,_p, _o = triple
            
            if _s.__class__.__name__ == 'SparqlVar':
                _s = _s.name
            
            if _o.__class__.__name__ == 'SparqlVar':
                _o = _o.name
            
            # Init incoming and outgoing adjacencies for a node, if not exist
            if not _s in adjacency_list:
                adjacency_list[_s] = []
            
            if not _o in adjacency_list:
                adjacency_list[_o] = []
            
            # Insert both connections
            adjacency_list[_s].append( (None,_p,_o) )
            adjacency_list[_o].append( (_s,_p,None) )
        
        # Main cycle. Yield 1 union each cycle
        # ====================================
        # The number of UNIONs is the number of triples that contain selected
        # variables
        unions = []
        for triple in parsed.where:
            _s ,_p, _o = triple
            if ( _s not in parsed.selected ) and\
               ( _o not in parsed.selected ): continue
            
            if _s.__class__.__name__ == 'SparqlVar': _s = _s.name
            if _o.__class__.__name__ == 'SparqlVar': _o = _o.name
            
            # Table name of the property of the current triple
            table_name = self.db.getPropertyTableName(_p)
                    
            if table_name:
                table_name = table_name[0]
            else:
                logger.critical('Table "%s" not found, but must be there!' % _p)
                return None
            
            # The first element after the timestamps is the removed flag
            select = []
            select.append('`%s`.%s AS removed' % (table_name, 'Removed'))
                    
            for var in selected_vars_sql:
                var_name = var['name']
                if var_name == _s:
                    # Get table name. If this handler has been called, the table
                    # must exist! If it doesn't it's a big problem!!!
                    table_name = self.db.getPropertyTableName(_p)
                    
                    if table_name:
                        table_name = table_name[0]
                    else:
                        logger.critical('Table "%s" not found, but must be there!' % _p)
                        return None
                    
                    select.append('`%s`.%s AS %s' % (table_name, 'SubjectID', var_name))
                    
                    # This is a subj so for sure is an URI
                    var['uri'] = True
                    
                elif var_name == _o:
                    table_name = self.db.getPropertyTableName(_p)
                    
                    if table_name:
                        table_name, _t = table_name
                    else:
                        logger.critical('Table "%s" not found, but must be there!' % _p)
                        return None
                    
                    select.append('`%s`.%s AS %s' % (table_name, 'Object', var_name))
                    
                    # Property type from table
                    var['uri'] = _t
                else:
                    select.append('NULL AS %s' % var_name)
    
            # 'NULL AS .., NULL AS ..., smth AS asdf'
            select_clause_sql.append(','.join(select))
            
            # Need table name, not the property name
            table_name = self.db.getPropertyTableName(_p)
            if table_name:
                table_name = table_name[0]
            else:
                logger.warning('Table "%s" not found, but handler has been called!' % _p)
                return None
            
            join = " JOIN `%(p)s` ON r.ID = `%(p)s`.RecordID" % ({'p':table_name})
            
            if _s.__class__.__name__ == 'SparqlVar': _s = _s.name
            if _o.__class__.__name__ == 'SparqlVar': _o = _o.name
            
            # Contains the nodes where to start the adjacencies walk and also
            # the path you come from, to reach that node: you don't want to walk
            # back on the same path!            
            nodes = {_s: (None,_p,_o), _o: (_s,_p,None)}
            
            # Iterate till the stack is not empty
            while len(nodes):
                # Randomly pop a key-value pair. Order doesn't matter!
                node, relFrom = nodes.popitem()
                
                for rel in adjacency_list[node]:
                    if not rel == relFrom:
                        _s,_p,_o = rel
                        
                        # Found a new node on the adjacencies walk. The
                        # returning path has the verse which is opposite to the
                        # adjacency
                        p = self.db.getPropertyTableName(_p)
                        p_from = self.db.getPropertyTableName(relFrom[1])
                        
                        if p and p_from:
                            p = p[0]
                            p_from = p_from[0]
                        else:
                            logger.warning('Tables "%s" or "%s" not found, but handler has been called!' 
                                % (_p,relFrom[1]))
                            return None
                        
                        # The JOIN on subj/obj depends on the positions
                        # of the None in 'rel' and 'relFrom'
                        # j_on/_from are set to 'SubjectID' or 'Object'
                        # This trick handle properties chaining in both direction
                        # and parallel properties
                        j_on      = join_on[     rel.index(None) ]
                        j_on_from = join_on[ relFrom.index(None) ]
                        
                        join_properties = {'p': p, 'p_from': p_from,
                                           'j_on': j_on, 'j_on_from': j_on_from}
                        
                        join_properties_debug = {'p': _p, 'p_from': relFrom[1],
                                                 'j_on': j_on, 'j_on_from': j_on_from}
                        
                        # Add the path to the query with a JOIN clause
                        join += """
 JOIN `%(p)s` ON `%(p_from)s`.%(j_on_from)s = `%(p)s`.%(j_on)s""" % ( 
                            join_properties)
 
                        _join = """
 JOIN `%(p)s` ON `%(p_from)s`.%(j_on)s = `%(p)s`.%(j_on_from)s""" % ( 
                            join_properties_debug)
 
                        # Incoming adjacency
                        if   _o == None:
                            nodes[_s] = (None, _p, node)
                        # Outgoing adjacency
                        elif _s == None:
                            nodes[_o] = (node, _p, None)
                        
            unions.append('SELECT r.ID AS rec, unix_timestamp(r.Timestamp), ' +
                          select_clause_sql.pop(0) +
                          ' FROM `Records` AS r' +
                          join )
            
        sql_query = ' UNION '.join(unions)
        sql_query+= ' ORDER BY rec ASC'
        cur = self.db.connection.cursor()
        cur.execute(sql_query)
        
        # SPARQL response: root
        sparql_response = ET.Element('sparql', 
                                {'xmlns':"http://www.w3.org/2005/sparql-results#"})
        
        # SPARQL response: head
        sparql_response_head = ET.SubElement(sparql_response, 'head')
        ET.SubElement(sparql_response_head,
            'variable', {'name': 'HistoryServiceTimestamp'})
        for var in parsed.variables:
            ET.SubElement(sparql_response_head, 'variable', {'name': var})
        
        # SPARQL response: results
        sparql_response_results = ET.SubElement(sparql_response, 'results')
        
        # row is something like:
        #
        # (2L, datetime.datetime(2013, 4, 17, 15, 7, 13), removed
        #  None, None, 2L, 1L, None, None, None, None)
        #
        # (RecordID, date, your_data, lot of NULL/None)
        #
        row = cur.fetchone()
        
        while row:
            sparql_response_result = ET.SubElement(sparql_response_results, 'result')
            for i,col in enumerate(row):
                # RecordID
                if   i == 0: continue
                
                # Record Timestamp
                elif i == 1:
                    sparql_response_result_binding = ET.SubElement(
                        sparql_response_result, 'binding',
                        {'name': 'HistoryServiceTimestamp'})
                    
                    sparql_response_result_binding_type = ET.SubElement(
                        sparql_response_result_binding, 'literal')
                    
                    sparql_response_result_binding_type.text = str(col)
                
                # Removed
                elif i == 2:
                    sparql_response_result_binding = ET.SubElement(
                        sparql_response_result, 'binding',
                        {'name': 'HistoryServiceRemoved'})
                    
                    sparql_response_result_binding_type = ET.SubElement(
                        sparql_response_result_binding, 'literal')
                    
                    sparql_response_result_binding_type.text = str(col)
                
                # Variables
                else:
                    if col == None: continue
                    
                    sparql_response_result_binding = ET.SubElement(
                                    sparql_response_result,
                                    'binding',
                                    { 'name': selected_vars_sql[i-3]['name'] } )
                    
                    if selected_vars_sql[i-3]['uri']: 
                        type = 'uri'
                        var_value = str( self.db.getInstanceURI(int(col))[0] )
                    else:
                        type = 'literal'
                        var_value = str(col)
                    
                    sparql_response_binding_type = ET.SubElement(
                        sparql_response_result_binding, type)
                    
                    sparql_response_binding_type.text = var_value
                    
            row = cur.fetchone()
        
        # Alert, ET.dump() is a print, use ET.tostring() to save in variable
        result = ET.tostring(sparql_response)
        
        insert = theNode.CreateInsertTransaction(theSmartSpace)
        insert.insert([
            Triple( URI(read_request_uri),
                    URI(HAS_HISTORY_READ_RESPONSE),
                    Literal(result) ) ])
        theNode.CloseQueryTransaction(insert)


####################################################################

def _getURI(string):
    """ Extracts content between angle brackets <urihere>
    """
    m = matchAngleBrackets.match(string)
    return (False if (m == None) else m.group(1))


####################################################################
logging.setLoggerClass(ColoredLogger)

global logger
logger = logging.getLogger('HistoryService')

#stream = logging.StreamHandler()

#format = logging.Formatter('%(levelname)s [%(name)s]: %(message)s')
#stream.setFormatter(format)

#logger.addHandler(stream)

def _usage():
    """
    Print the usage page. Accessed running the script with -h or --help
    """
    
    print """
-h, --help : Show this help page
-r, --rebuild-db : Drop the history DB and reinitialize the DB and all the tables
-l, --log: Set log level. Case insensitive options: DEBUG, INFO, WARNING, 
    ERROR, CRITICAL.
    """
            
def _set_commandline_options(argv):
    try:
        # Parse options
        opts, args = getopt.getopt(argv, "hrl:d:", 
            ["help", "rebuild-db", "log=", "db="])
        
        # Set options
        for opt, arg in opts:
            
            # Help and exit
            if opt in ("-h", "--help"):
                _usage()
                sys.exit()
                
            # Reset DB
            elif opt in ("-r", "--rebuild-db"):
                rebuild_db = True
    
            # Set log level
            elif opt in ('-l', '--log'):
                global numeric_level
                numeric_level = getattr(logging, arg.upper(), None)
                if not isinstance(numeric_level, int):
                    raise ValueError('Invalid log level: %s' % arg)
                logger.setLevel(numeric_level)
            
            # DB
            elif opt in ('-d', '--db'):
                database = arg
        
    except getopt.GetoptError:
        _usage()
        sys.exit(2)
        
    return opts, args


if __name__ == "__main__":
    
    # Set from command line
    SmartSpaceName = "X"
    IPADDR = "localhost"
    Port = 10010
    
    nodename=str(uuid.uuid4())
    theNode=KP(nodename)
    
    theSmartSpace=(SmartSpaceName,(TCPConnector,(IPADDR,Port)))
    if not theNode.join(theSmartSpace):
        sys.exit("Could not join to Smart Space")
    print "*** Joined ("+str(nodename)+") with SmartSpace "+str(theSmartSpace)+" ***"
        
    main(sys.argv[1:])