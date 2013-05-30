class HistorySparqlHandler():
    """Handler for SPARQL notification of history request
    """
    
    def __init__(self, templates):
        
        global DB, M3
        self.db = DB
        self.m3 = M3        
        
        global logger
        #self.logger = logger
        
        self.templates = templates
    
    def handle(self, added, removed):
        """
        """
        
        # The order here is of PARAMOUNT importance! You need to remove prior
        # of inserting new triples!!!!
        if removed and removed != []:
            to_remove = self.build_triples_to_remove(removed)
            logger.debug(to_remove)
            self.db.removeTriples(to_remove)
        
        if added and added != []:            
            to_add = self.build_triples_to_add(added)
            logger.debug(to_add)
            self.db.addTriples(to_add)
    

    def build_triples_to_remove(self, removed_vars):
        
        vars = self.prettify_sparql_result(removed_vars)
        
        # Sparql query translated in RDF query, where all triples are in OR
        # chain
        rdf_query = self.sparql2rdf(self.templates, vars)
        
        # RDF query to understand what has been really deleted
        #qTransaction = theNode.CreateQueryTransaction(theSmartSpace)
        result = self.m3.load_rdf_query(rdf_query)
        #result = qTransaction.rdf_query(rdf_query)
        #theNode.CloseQueryTransaction(qTransaction)
        
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
            t.append(_uri)
            
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
                    
                    #triple_to_write.append(type)
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