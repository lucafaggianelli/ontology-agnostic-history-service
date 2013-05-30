import fyzz
from HistorySparqlHandler import *

class HistoryRequestHandler():
    """Req handler
    """
    
    def __init__(self, m3, logger):
        
        self.m3 = m3
        self.logger = logger
    
    
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
            result = self.m3.load_query_sparql(q)
            #result = query.sparql_query(q)
            
            if len(result):
                sparqlToSubscribeTo = str(result[0][0][2])
                
                # If SPARQL is not good remove the request and continue the loop
                parsed = fyzz.parse(sparqlToSubscribeTo)
    
                # Subscribe to SPARQL query issued by the history request
                #subscription = theNode.CreateSubscribeTransaction(theSmartSpace)
                subscription = self.M3.load_subscribe_sparql(sparqlToSubscribeTo,
                                            HistorySparqlHandler(parsed.where))
                #results = subscription.subscribe_sparql(sparqlToSubscribeTo, 
                #                              HistorySparqlHandler(parsed.where))
                logger.info("Subscribed to: %s" % sparqlToSubscribeTo)
                
                # Serve the history request with existing data, otherwise when 
                # the read request gets an error (table not found)
                HistorySparqlHandler(parsed.where).handle(self.m3.result_sparql_first_sub,
                                                          [])
                
                # Track all the subscriptions in an array
                global sparqlSubscriptions
                sparqlSubscriptions.append(subscription)
            else:
                logger.warning('Request has no sparql query to subscribe to')

        for index in removed:
            logger.info("Removed record:")
            logger.info(index)