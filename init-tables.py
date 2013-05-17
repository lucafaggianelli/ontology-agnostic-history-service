import sys, getopt
import string, random

from OntologyAnalyzer import *
from DatabaseWriter import *

def main(argv):
    """
    """
    
    # == Setting options
    # Debug level
    global _DEBUG; _DEBUG = False
    # Drop the DB and build it again
    rebuild_db = False
    # Input ontology
    ontology = '/home/luca/ontologies/person-car/person-car.owl'

    opts, args = parse_args(argv)
    for opt, arg in opts:
        # Long options
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-r", "--rebuild-db"):
            rebuild_db = True
        elif opt in ("-o", "--ontology"):
            ontology = arg

        # Short only opts
        elif opt == '-d':    
            _DEBUG = True
    
    print ''.join(args)


    # == Start serious business!

    result = analyze_ontology(ontology)

    build_tables(result, rebuild_db)



# == Ontology Analisys

def analyze_ontology(ontology_name):
    
    analyzer = OntologyAnalyzer(ontology_name)

    # Init vars
    ontology_features = {
        'classes': None,
        'instances': None,
        
        'objectProperties': None,
        'datatypeProperties': None
    }

    # == Find Classes
    # Classes types not necessary, function default are owl:Class and rdfs:Class
    ontology_features['classes'] = analyzer.getClasses()

    # == Find instances
    # Classes type default to RDFS and OWL Class
    ontology_features['instances'] = analyzer.getInstances()
    
    # == Find Object Properties
    # Function defaults to 'owl:ObjectProperty'
    ontology_features['objectProperties'] = analyzer.getProperties()    

    # == Find DatatypeProerties
    # Function defaults to 'owl:ObjectProperty', so specify type!
    ontology_features['datatypeProperties'] = analyzer.getProperties(
                                                ('owl:DatatypeProperty',))
    
    return ontology_features


# == DataBase Operations

def build_tables(ontology, rebuild_db):

    # == DB initialization
    db = DatabaseWriter('localhost', 'root', 'luca123', 'history', rebuild_db);

    # = Datatype Properties
    for p in ontology['datatypeProperties']:
        db.createDatatypePropertyTable(p)
    
    # = Object Properties
    for p in ontology['objectProperties']:
        db.createObjectPropertyTable(p)
    
    # Insert already existing instances  
    for i in ontology['instances']:
        db.createInstance(i['instance'], i['class'])



def usage():
    """Print the usage page. Accessed running the script with -h or --help
    """
    
    print """
-h, --help : Show this help page
-r, --rebuild-db : Drop the history DB and reinitialize the DB and all the tables
-o, --ontology=file.owl : Specifies the input ontology
-d : Print debug messages
    """

def _debug(msg):
    if _DEBUG:
        print msg, "\n"


def parse_args(argv):
    try:
        opts, args = getopt.getopt(argv, "hro:d", 
                                   ["help", "rebuild-db", "ontology="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
        
    return opts, args

if __name__ == '__main__':
    """Executed when script is run from the terminal, false when it is included
    as module.
    It parses the command line arguments
    """
    main(sys.argv[1:])