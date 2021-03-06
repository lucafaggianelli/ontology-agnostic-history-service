from smart_m3.RDFTransactionList import *
from Utility.Ontology import *
from smart_m3.m3_kp import *
from smart_m3.m3_kp_api import *

#from DatabaseWriter import *

import uuid
import sys

global M3
M3 = m3_kp_api(False, 'localhost')#'192.168.1.104')

class HistoryClient():
    """This class is a Client Interface for the History Service. It should
    be imported in your own project as a module, anyway a primitive User
    Interface is available in the command line, so the script may be executed
    directly.
    """
    
    def __init__(self):
        
        global M3
        self.m3 = M3
        self.readResponseSubscriptions = []
        
        
    def readHistoryData(self, sparql, handler = None):
        """Execute a generic SPARQL query in the History DB. It is not
        mandatory that a History Request with the same SPARQL query exists.
        
        **Arguments**
            * sparql (str) - SPARQL query
            * handler (Class) - Handler of the response. None if dont want to
            subscribe, can fetch the response in a second moment, but please
            clear the read request calling .clearReadRequest(req_id).
            
        **Return**
            * (str) - Read request instance URI.
        """

        history_read_instance = HISTORY_READ+'_'+str(uuid.uuid4())
        
        # Subscribe to the response so you're sure to catch it...it may
        # be too fast!
        if handler:
            response_sub_query = 'SELECT ?res WHERE {<%s> <%s> ?res}' % (
                history_read_instance, HAS_HISTORY_READ_RESPONSE)
            
            # Store the subscription, to unsubscribe later...
            # TODO: Clear the request from the SIB and unsubscribe, this should be
            # done in the handler: build a superclass that does the trick!
            sub = self.m3.load_subscribe_sparql(response_sub_query, 
                                          HistoryReadResponse(handler,
                                                              history_read_instance))
            self.readResponseSubscriptions.append(sub)
            
            
        insert = [
            Triple(
                URI(history_read_instance),
                URI('rdf:type'),
                URI(HISTORY_READ) ),
        
            Triple(
                URI(HISTORY),
                URI(HAS_HISTORY_READ),
                URI(history_read_instance) ),
        
            Triple(
                URI(history_read_instance),
                URI(HAS_SPARQL),
                Literal(sparql) )]
        
        self.m3.load_rdf_insert(insert)
        
        return history_read_instance
        
    
    def readHistoryRequestData(self, request_uri, handler = None):
        """Execute a SPARQL query relative to a History Request.
        
        **Arguments**
            * request_uri (str) - URI of the request, a HistoryInput instance
            * handler (Class) - Handler of the response. None if dont want to
            subscribe, can fetch the response in a second moment.
            
        **Return**
            * (str) - Read request instance URI
        """
        
        sparql = self.showHistotyRequestDetails(request_uri)
        return self.readHistoryData(sparql, handler)
    
    
    def clearHistoryReadRequest(self, req_id):
        """Clear an history read request from the history service. To be
        called only if you don't provide a handler to the history read request
        """
        
        self.m3.load_rdf_remove([Triple(
                                    URI(self.req_id),
                                    URI(HAS_HISTORY_READ_RESPONSE),
                                    None)])
        
    
    def addHistoryRequest(self, sparql):
        """Issues a History Request
        
        **Arguments**
            * sparql (str) - SPARQL query whose variables must be tracked
            
        **Return**
            * (bool) - True to confirm of the request, False otherwise
        """
                
        history_input_instance = HISTORY_INPUT+'_'+str(uuid.uuid4())
        
        insert = [
            Triple(
                URI(history_input_instance),
                URI('rdf:type'),
                URI(HISTORY_INPUT) ),
        
            Triple(
                URI(HISTORY),
                URI(HAS_HISTORY_INPUT),
                URI(history_input_instance) ),
        
            Triple(
                URI(history_input_instance),
                URI(HAS_SPARQL),
                Literal(sparql) )]
        
        self.m3.load_rdf_insert(insert)
        
        return history_input_instance
        
        
    def deleteHistoryRequest(self, req_uri):
        """Execute a generic SPARQL query in the History DB. It is not
        mandatory that a History Request with the same SPARQL query exists.
        
        **Arguments**
            * request_uri (str) - URI of the request, a HistoryInput instance
            
        **Return**
            * (bool) - True to confirm the deletion, False otherwise
        """
        
        rem = []
        
        history_input_instance = req_uri
        
        rem.append(Triple(
            URI(history_input_instance),
            URI('rdf:type'),
            URI(HISTORY_INPUT) ))
        
        rem.append(Triple(
            URI(HISTORY),
            URI(HAS_HISTORY_INPUT),
            URI(history_input_instance) ))
        
        rem.append(Triple(
            URI(history_input_instance),
            URI(HAS_SPARQL),
            None ))
        
        self.m3.load_rdf_remove(rem)
    
    
    def showHistotyRequestDetails(self, request_uri):
        """Shows the details of a History Request: backend DB and user,
        sampling frequency, SPARQL... Now only SPARQL is supported
        
        **Arguments**
            * request_uri (str) - URI of the request
            
        **Return**
            * (hash) - Details of the request
        """
        
        query = "select ?sparql where {\
            <%s> <%s> ?sparql .}" % (request_uri, HAS_SPARQL)
            
        self.m3.load_query_sparql(query)
        
        return self.m3.result_sparql_query[0][0][2]


    def showHistoryRequests(self):
        """Lists all the requests found in the SIB, showing only the SPARQL
        query associated to it. If all details are needed, please use 
        :func:`showHistotyRequestDetails` 
        
        **Arguments**
            None
            
        **Return**
            * (hash) - Result of the query with timing information
        """
        
        query = "select ?hi ?sparql where {\
            <%s> <%s> ?hi .\
            ?hi <%s> ?sparql .}" % (HISTORY, HAS_HISTORY_INPUT, HAS_SPARQL)
            
        self.m3.load_query_sparql(query)
        
        requests = []
        for req in self.m3.result_sparql_query:
            requests.append({'uri': req[0][2], 'sparql': req[1][2]})
        
        return requests


    def addHistoryDB(self, user, psw, rdbms = 'mysql', host = 'localhost',
                     name = 'history'):
        
        
        history_db_instance = HISTORY_DB+'_'+str(uuid.uuid4())
        
        insert = [
            Triple(
                URI(history_db_instance),
                URI('rdf:type'),
                URI(HISTORY_DB) ),                  
            Triple(URI(history_db_instance), URI(HAS_RDBMS), Literal(rdbms)),
            Triple(URI(history_db_instance), URI(HAS_DB_HOST), Literal(host)),
            Triple(URI(history_db_instance), URI(HAS_DB_NAME), Literal(name)),
            Triple(URI(history_db_instance), URI(HAS_DB_USER), Literal(user)),
            Triple(URI(history_db_instance), URI(HAS_DB_PASS), Literal(psw))]
        
        self.m3.load_rdf_insert(insert)
        
        return True
    
    
    def showHistoryDBs(self, history_db_uri = None):

        sparql = "select ?p ?o {"
        
        if not history_db_uri:
            sparql += "?hdb rdf:type <%s>. ?hdb ?p ?o.}" % (HISTORY_DB)
        else:
            sparql += "<%s> ?p ?o.}" % history_db_uri
        
        self.m3.load_query_sparql(sparql)
        
        return self.m3.result_sparql_query
    
    
    def quit(self):
        """Call this method before quiting your app. It executes cleanup
        and subscriptions close"""
        
        for sub in self.readResponseSubscriptions:
            self.m3.load_unsubscribe(sub)


class HistoryReadResponse():
    """Auxiliary class used to automatically remove the read request
    """
    
    
    def __init__(self, handler, req_id):
        self.handler = handler
        self.req_id = req_id
    
    def handle(self, added, removed):
        
        global M3
        
        # Remove History read request automatically
        M3.load_rdf_remove([Triple(
                                URI(self.req_id),
                                URI(HAS_HISTORY_READ_RESPONSE),
                                None)])
        
        # Call the handler
        self.handler.handle(added,removed)
        
              

def main():
    
    client = HistoryClient()
    menu = '\n'.join((
        '1 - Make a history request',
        '2 - Remove history request',
        '3 - Show all history requests',
        '4 - Read historical request',
        '5 - Read historical data',
        '* - Quit\n'
    ))
    
    while(True):
        choice = raw_input(menu)
        
        choice = int(choice)
        
        if choice == 1:
            sparql = raw_input('Sparql (USE CAPITALS!): ')
            client.addHistoryRequest(sparql)
            
        elif choice == 2:
            req_id = raw_input('History request uri: ')
            client.deleteHistoryRequest(req_id)
            
        elif choice == 3:
            print client.showHistoryRequests()
            
        elif choice == 4:
            request_uri = raw_input('History request uri: ')
            print client.readHistoryRequestData(request_uri)
        
        elif choice == 5:
            history_sparql = raw_input('Sparql (USE CAPITALS!): ')
            print client.readHistoryData(history_sparql)
            
        else: sys.exit()
         

if __name__ == "__main__":
    main()