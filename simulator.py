from smart_m3.RDFTransactionList import *
from Utility.Ontology import *
from smart_m3.m3_kp_api import *
import sys
from threading import Thread
import time

ns = 'http://rdf.tesladocet.com/ns/person-car.owl#'

def main():
    tachometer = Tachometer()
    tachometer.start()
    
    global carEngineOn
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print 'Shutting off the car...'
        carEngineOn = False
        sys.exit()
    
class Tachometer(Thread):
    def __init__(self):
        super(Tachometer, self).__init__()
        self.km = 0
        self.m3 = m3_kp_api()
        
        global carEngineOn
        carEngineOn = True
        
    def run(self):
        
        # Person's car
        remove = [
            Triple(URI(ns+'Person_1'),
                   URI(ns+'HasCar'),
                   None)]
        insert = [
            Triple( URI(ns+'Person_1'), 
                    URI(ns+'HasCar'), 
                    URI(ns+'Car_1') )]
        self.m3.load_rdf_update(insert, remove)
        print 'Owners info inserted'
        
        # Car's tires
        remove = [
            Triple( URI(ns+'Car_1'), 
                    URI(ns+'HasTire'), 
                    None ),
            Triple( URI(ns+'Pirelli_4'), 
                    URI(ns+'HasTireTread'), 
                    None )]
        insert = [
            Triple( URI(ns+'Car_1'), 
                    URI(ns+'HasTire'), 
                    URI(ns+'Pirelli_4') ),
            Triple( URI(ns+'Pirelli_4'), 
                    URI(ns+'HasTireTread'), 
                    URI(ns+'TireTread_Wet') )]
        self.m3.load_rdf_update(insert, remove)
        print 'Tire info inserted'

        global carEngineOn
        while(carEngineOn):
            self.km += 1
            
            insert = [Triple(URI(ns+'Car_1'),URI(ns+'HasKm'),Literal(self.km))]
            remove = [Triple(URI(ns+'Car_1'),URI(ns+'HasKm'),None)]
            
            self.m3.load_rdf_update(insert, remove)
            
            print "Car run for %ikm" % self.km
            
            time.sleep(3)
            
        print 'Now is off!'
        


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