import rdflib
from rdflib import Graph, OWL, RDF, RDFS, XSD
# Enable SPARQL query
from rdflib import plugin
plugin.register(
    'sparql', rdflib.query.Processor,
    'rdfextras.sparql.processor', 'Processor')
plugin.register(
    'sparql', rdflib.query.Result,
    'rdfextras.sparql.query', 'SPARQLQueryResult')


class OntologyAnalyzer:
    """
    """
    
    def __init__(self, ontology, opts = {}):
        """
        ontology -- File name of the ontology to analyze
        """
        if not ontology: raise ValueError
        
        self.graph = Graph()
        self.graph.load(ontology)
        
        # Bind default NS to prefix on graph
        self.graph.bind('owl',  OWL)
        self.graph.bind('rdf',  RDF)
        self.graph.bind('rdfs', RDFS)
        self.graph.bind('xsd',  XSD)
        
    
    def getClasses(self, types = ('owl:Class', 'rdfs:Class')):
        """ Returns all the classes found in an ontology

        types -- Tuple or list of class types as strings. [optional] 
                   default ('owl:Class','rdfs:Class')
        ---
        return -- List of classes as strings ['class1', 'class2']
        """
        
        sparql = ''
        for index, t in enumerate(types):
            if index: sparql += ' UNION ' # i=0 -> {...} | i=1 -> UNION {...} | ...
            sparql += "{ ?class a %s }" % t
        
        if len(types) > 1:
            sparql = '{'+sparql+'}'
        
        sparql = 'select ?class where ' + sparql
        
        # Execute SPARQL query: registered at the beginning on the rdflib.
        # Returns rdfextras.sparql.query.SPARQLQueryResult object that must 
        # be iterated
        rows = self.graph.query(sparql)
        
        # Iterate rows for parsing
        # r is a Tuple of length = to the number of SPARQL vars
        classes = []
        for r in rows:
            # Get class full uri, str() for reliability
            classes.append( str(r[0]) )
        
        return classes

    
    def getInstances(self, types = ('owl:Class', 'rdfs:Class')):
        """ Returns all the instances found in a graph, defined by 
            <instance> rdf:type <class>

        types -- Tuple or list of class types as strings. [optional] 
                   default ('owl:Class','rdfs:Class')
        ---
        return -- List of instances as objects [{'instance': '', 'class': ''},{...}]
        """
        
        sparql = ''
        for index, t in enumerate(types):
            if index: sparql += ' UNION ' # i=0 -> {...} | i=1 -> UNION {...} | ...
            sparql += "{ ?inst a ?class . ?class a %s }" % t
            
        if len(types) > 1:
            sparql = '{'+sparql+'}'
        
        sparql = 'select ?inst ?class where ' + sparql
    
        # Execute SPARQL query: registered at the beginning on the rdflib.
        # Returns rdfextras.sparql.query.SPARQLQueryResult object that must 
        # be iterated
        rows = self.graph.query(sparql)
        
        # Iterate rows for parsing
        # r is a Tuple of length = to the number of SPARQL vars
        instances = []
        for r in rows:
            instances.append({ 'instance': str(r[0]), 'class': str(r[1]) })
            
        return instances


    def getProperties(self, types = ('owl:ObjectProperty',)):
        """ Returns all the properties found in a graph
          
        types -- Tuple or list of properties types as strings. [optional] 
                   default ('owl:ObjectProperty')
        ---
        return -- List of properties as strings ['prop1', 'prop2']
        """
        
        sparql = ''
        for index, t in enumerate(types):
            if index: sparql += ' UNION ' # i=0 -> {...} | i=1 -> UNION {...} | ...
            sparql += "{ ?prop a %s }" % t
            
        if len(types) > 1:
            sparql = '{'+sparql+'}'
        
        sparql = 'select ?prop where ' + sparql
        
        # Execute SPARQL query: registered at the beginning on the rdflib.
        # Returns rdfextras.sparql.query.SPARQLQueryResult object that must 
        # be iterated
        rows = self.graph.query(sparql)
        
        # Iterate rows for parsing
        # r is a Tuple of length = to the number of SPARQL vars
        properties = []
        for r in rows:
            properties.append( str(r[0]) )
            
        return properties