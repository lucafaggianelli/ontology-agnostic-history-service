import fyzz
import xml.etree.ElementTree as ET

class HistoryReadRequestHandler():
    """Read handler
    Test command in python prompt:
    from HistoryService import *; r = HistoryReadRequestHandler(); logger.setLevel(4); r.readHistoryData(r.test)
    """

    def __init__(self):

        global DB,M3
        self.db = DB
        self.m3 = M3
        
        global logger
        self.logger = logger
        
        self.test = """
            SELECT ?person ?car ?km ?tire ?tireTread WHERE {
                ?person <http://rdf.tesladocet.com/ns/person-car.owl#HasCar>   ?car .
                ?car    <http://rdf.tesladocet.com/ns/person-car.owl#HasKm> '7' .
                ?car    <http://rdf.tesladocet.com/ns/person-car.owl#HasTire> ?tire .
                ?tire   <http://rdf.tesladocet.com/ns/person-car.owl#HasTireTread> ?tireTread
            }"""
    
    def handle(self, added, removed):
        """M3 Handler
        """
        for read_request in added:
            
            #read = theNode.CreateQueryTransaction(theSmartSpace)
            result = self.m3.load_rdf_query([
                Triple( URI(read_request[2]),
                        URI(HAS_SPARQL),
                        None ) ])
            #theNode.CloseQueryTransaction(read)
            
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
        
        # Columns name used for joining to property table
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
                        
            unions.append('SELECT r.ID, unix_timestamp(r.Timestamp), ' +
                          select_clause_sql.pop(0) +
                          ' FROM `Records` AS r' +
                          join)
            
        sql_query = ' UNION '.join(unions)
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
                        var_value = str( self.db.getInstanceURI(int(col)) )
                    else:
                        type = 'literal'
                        var_value = str(col)
                    
                    sparql_response_binding_type = ET.SubElement(
                        sparql_response_result_binding, type)
                    
                    sparql_response_binding_type.text = var_value
                    
            row = cur.fetchone()
        
        # Alert, ET.dump() is a print, use ET.tostring() to save in variable
        result = ET.tostring(sparql_response)
        
        #insert = theNode.CreateInsertTransaction(theSmartSpace)
        self.m3.load_rdf_insert([
            Triple( URI(read_request_uri),
                    URI(HAS_HISTORY_READ_RESPONSE),
                    Literal(result) ) ])
        #theNode.CloseQueryTransaction(insert)