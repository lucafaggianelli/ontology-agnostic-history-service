from smart_m3.RDFTransactionList import *
from Utility.Ontology import *
from smart_m3.m3_kp_api import *

from HistoryClient import *

import sys
from threading import Thread
import time

ns = 'http://rdf.tesladocet.com/ns/person-car.owl#'

def main():
    
    client = HistoryClient()
    
    print '-- Car history simulator --'
    
    # Make a history request for data we're gonna insert
    sparql = """
        SELECT ?person ?car ?km ?tire ?tireTread WHERE {
            ?person <http://rdf.tesladocet.com/ns/person-car.owl#HasCar>   ?car .
            ?car    <http://rdf.tesladocet.com/ns/person-car.owl#HasKm> ?km .
            ?car    <http://rdf.tesladocet.com/ns/person-car.owl#HasTire> ?tire .
            ?tire   <http://rdf.tesladocet.com/ns/person-car.owl#HasTireTread> ?tireTread
        }"""
    req_id = client.addHistoryRequest(sparql)
    print req_id
    
    # Start the simulator
    tachometer = Tachometer()
    tachometer.start()
    
    # Keep it running untill Ctrl-c
    global carEngineOn
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print 'Shutting off the engine...'
        carEngineOn = False
        
    # Retrieve back historical data
    client.readHistoryRequestData(req_id, ReadResponseHandler())
    
    # Close all the subscriptions of the client, otherwise it hangs
    client.quit()
    
    sys.exit()
    
    
class Tachometer(Thread):
    def __init__(self):
        super(Tachometer, self).__init__()
        self.km = 0
        self.m3 = m3_kp_api()
        
        global carEngineOn
        carEngineOn = True
        
    def run(self):
        
        # Car info
        remove = [
            Triple(URI(ns+'Person_1'),
                   URI(ns+'HasCar'),
                   None),
            Triple( URI(ns+'Car_1'), 
                    URI(ns+'HasTire'), 
                    None ),
            Triple( URI(ns+'Pirelli_4'), 
                    URI(ns+'HasTireTread'), 
                    None )]
        insert = [
            Triple( URI(ns+'Person_1'), 
                    URI(ns+'HasCar'), 
                    URI(ns+'Car_1') ),
            Triple( URI(ns+'Car_1'), 
                    URI(ns+'HasTire'), 
                    URI(ns+'Pirelli_4') ),
            Triple( URI(ns+'Pirelli_4'), 
                    URI(ns+'HasTireTread'), 
                    URI(ns+'TireTread_Wet') )]
        self.m3.load_rdf_update(insert, remove)
        print 'Car info:'
        print insert

        global carEngineOn
        while(carEngineOn):
            self.km += 1
            
            insert = [Triple(URI(ns+'Car_1'),URI(ns+'HasKm'),Literal(self.km))]
            remove = [Triple(URI(ns+'Car_1'),URI(ns+'HasKm'),None)]
            
            self.m3.load_rdf_update(insert, remove)
            
            print "Car runs for %ikm" % self.km
            
            time.sleep(3)
            
        print 'Engine is off!'
        
        
class ReadResponseHandler():
    
    def handle(self, added, removed):
        # removed should be []
        # added should contain only 1 result with 1 variable
        # [ [['res', 'literal', 'your_xml_response']] ]
        
        response = parse_sparql(added[0][0][2])
        print response
        
        for i,result in enumerate(response):
            print '\n'
            for var in result:
                print str(i)+') '+var[0]+' = '+var[2]+'; '+var[1]
                


if __name__ == "__main__":
    
    SmartSpaceName = "X"
    IPADDR = "localhost"
    Port = 10010

    print "\nSmart Space Access\n"
    nodename=str(uuid.uuid4())
    theNode=KP(nodename)
    
    theSmartSpace=(SmartSpaceName,(TCPConnector,(IPADDR,Port)))
    if not theNode.join(theSmartSpace):
        sys.exit("Could not join to Smart Space")
    print "*** Joined ("+str(nodename)+") with SmartSpace "+str(theSmartSpace)+" ***"
        
    main()