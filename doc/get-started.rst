Get Started
===========

The `History Service` (or `Ontology Aware History Service`) is a service for
Smart M3 that keeps track of the triples insertion in the store, in an
`event-driven` way.

Its usage scenario features a SIB where data is written in a `live fashion`
(i.e.: the value of a temperature sensor is overwritten keeping only the last
sample) and a centralized, fast and reliable archiviation system is needed.

Tracking is made on specific requests, that is, the user should issue a
`History Request` in the form of a SPARQL query: the selected variables
and the triples involving them, will be stored in the DB together with a
timestamp at which the event occurred.

On the other way, the `Service` accepts `History Read Requests`, issued in
the form of SPARQL query **executed on the DB** and not on the SIB. 


What do I need?
---------------

  * `Smart M3 <http://sourceforge.net/projects/smart-m3/>`_,
    `here <http://sourceforge.net/projects/smart-m3/files/Smart-M3-RedSIB_0.4/>`_
    the last version:
  
    * Smart M3 RedSIB and TCP version >= 0.4
    * Smart M3 KPI version >= 1.0
  
  
  * SQL compliant Relational DB Management System. MySQL is supported 
    `out-of-the-box`, but other DB adapters may be easily written:
  
    * MySQL
    
    
  * Python2 >= 2.6. Please note that Python3 is not supported! If your 
    application need it, use a migration tool (2to3).
    
    * The History Service package


Run the History Service
-----------------------

* Start Smart M3 SIB daemon: ``redsibd`` in a terminal

* Start Smart M3 SIB TCP connector to access the SIB: ``sib-tcp`` in another 
  terminal
  
* Start the RDBMS daemon (for mysql: ``mysqld_safe``)

* Start the History Service daemon: ``python2 HistoryService.py``

Now the History Service is ready to accepts `History Requests` and 
`History Read Requests`. For this purpose, you need the `HistoryClient` 
module. If you launch it from the command line (``python2 HistoryClient.py``),
a raw interface is implemented. Anyway its real usage is to import it in your
application: on this purpose you can run the example application
``python2 HistoryClientGUI.py``, which implement a GUI for the client.

And now?
--------

If you didn't already download the History Service, then go to the :doc:`download` page.

If you're interested in how the History Service works, you may read the
:doc:`behind-scene` page.

If you are developing an application using the History Service, please read
the :doc:`reference`.