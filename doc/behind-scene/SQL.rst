SQL
===

Fundamental tables
------------------

The History Service needs a DB with at least three tables: *Records*,
*Instances* and *NamesDictionary*. Without these, the History Service Daemon
cannot start!


.. _sqltable-records:

Records table
~~~~~~~~~~~~~

The *Records* table has two columns: **ID** (Int, Primary Key, Auto Increment)
and **Timestamp** (Timestamp, Not NULL). Its purpose is to track all the events,
associating an unique ID to the time instant in which the event occurred,
then all information present in the DB will use such ID to refer to the time
instant. Simultaneous events share the same Record ID

+---+-----------+
|ID | Timestamp |
+===+===========+
| 1 | 1363287038|
+---+-----------+
| 2 | 1363287040|
+---+-----------+


.. _sqltable-instances:

Instances table
~~~~~~~~~~~~~~~

The *Instances* table features three columns: **ID** (Int, Primary Key, Auto Increment),
**class** (varchar(255), NULL), **instance** (varchar(255), NOT NULL) and
**Removed** (boolean, NOT NULL).
It contains all instances of either RDF or OWL classes, anyway the *class*
field is optional, thus the table registers all non-literal nodes (URIs).
The *Removed* flag allows instances deletion from the graph, it is of
paramount importance for the data duplication issue.

+---+---------------------------+-----------------------------+---------+
|ID |        class              | instance                    | Removed |
+===+===========================+=============================+=========+
| 1 | http://example.com/Car    | http://example.com/Car_7    | False   |
+---+---------------------------+-----------------------------+---------+
| 2 | http://example.com/Person | http://example.com/Person_4 | False   |
+---+---------------------------+-----------------------------+---------+
| 3 | http://example.com/Person | http://example.com/Person_4 | True    |
+---+---------------------------+-----------------------------+---------+



.. _sqltable-namesdictionary:

NamesDictionary table
~~~~~~~~~~~~~~~~~~~~~

Its existence is due to the properties tables naming system. The History
Service expects each property table to have the name of the property it maps,
but this is not possible due the limit in size of the names (for more information
please see :ref:`naming-system`), so at the end, tables have random IDs associated
to properties URI:

+---+---------------------------+-----------------------+
|ID |        Property           | TableName             |
+===+===========================+=======================+
| 1 | http://example.com/hasCar | qwertyuiop1234567890  |
+---+---------------------------+-----------------------+
| 2 | http://example.com/hasKm  | something32charslong  |
+---+---------------------------+-----------------------+



SPARQL to SQL
-------------

It is accomplished on a *History Read Request*, when a user supplies a SPARQL
query which must be executed on the *History Repository*, but this one is a
SQL compliant DB, so a translation in needed.

Lets take in consideration a simple SPARQL query, consisting only of the
``WHERE`` clause and one group

.. code-block:: sql

    SELECT ?person ?car ?brand WHERE {
        ?person <http://hasCar>   ?car .
        ?car    <http://hasBrand> ?brand
    }

Being an event-driven repository service, the *History Service* must respond
to the request with a sequence of events, thus the first task is to select the
timestamps and then the desired information. The result should be something
like this:

==== ====== ==== ==== =====
Time Person Car  Car  Brand
==== ====== ==== ==== =====
1    NULL   NULL Car1 BMW
2    Luca   Car1 NULL NULL
==== ====== ==== ==== =====

    **Please note**: the same result could be presented in one row and three
    columns (Person: Luca, Car: Car1, Brand: BMW), but this would break the
    event-driven approach, as *Car1* production and ownership are two events
    separated in time


The algorithm may be roughly resumed as:

  * The SQL query is given by N ``SELECT ... UNION`` clause, as N
    is the number of triples in a group of the SPARQL query
  * Properties map to tables
  * Repetition of the same variables leads to a ``JOIN``
  * Variables would lead to a, neglected, ``WHERE x = *`` clause
  * Fixed subjects/objects add a ``WHERE`` clause


The above SPARQL query yields the following SQL query:

.. code-block:: sql

    SELECT r.ID, r.Timestamp, hc.SubjectID as user, hc.Object as car, NULL as car, NULL as brand FROM `Records` AS r
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasCar` AS hc ON r.ID = hc.RecordID
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasBrand` AS hb ON hb.SubjectID = hc.Object

    UNION

    SELECT r.ID, r.Timestamp, NULL as user, NULL as car, hb.SubjectID as car, hb.Object as brand FROM `Records` AS r
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasBrand` AS hb ON r.ID = hb.RecordID
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasCar` AS hc ON hc.Object = hb.SubjectID


In both ``SELECT`` queries only the ``JOIN`` on the ``Record`` table is
essential, while the other(s) are needed due to the ``.`` (dot) in the
SPARQL query which is the *AND* operator, thus they ensure that the following
are not selected:

    * Car owned by a person, but without a brand
    * Car with a brand, but owned by no one

In particular, in absence of the second ``JOIN``, the car without a brand would
be selected by the first ``SELECT`` while the car without an owner, by the second
one.

Let's now complicate the query, which now become:

.. code-block:: sql

    SELECT ?person ?car ?brand ?tire ?tireTread WHERE {
        ?person <http://example.com/hasCar>   ?car .
        ?car    <http://example.com/hasBrand> ?brand .
        ?car    <http://example.com/hasTire> ?tire .
        ?tire   <http://example.com/hasTireTread> ?tireTread
    }

The above yields the following SQL query. Please notice the 4 ``UNION``, but
there are 5 vars: he number of ``UNION`` is actually given by the number of triples whose subject
and/or object are in the ``SELECT`` clause of the SPARQL query,
while the number of ``JOIN`` is the the number of triples.

.. code-block:: sql

    SELECT r.ID, r.Timestamp, hc.SubjectID as user, hc.Object as car, NULL as car, NULL as brand, NULL as car, NULL as tire, NULL as tire, NULL as tread FROM `Records` AS r
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasCar` AS hc ON r.ID = hc.RecordID
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasBrand` AS hb ON hb.SubjectID = hc.Object
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasTire` AS ht ON ht.SubjectID = hc.Object
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasTireTread` AS htt ON htt.SubjectID = ht.Object

    UNION

    SELECT r.ID, r.Timestamp, NULL as user, NULL as car, hb.SubjectID as car, hb.Object as brand, NULL as car, NULL as tire, NULL as tire, NULL as tread FROM `Records` AS r
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasBrand` AS hb ON r.ID = hb.RecordID
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasCar` AS hc ON hb.SubjectID = hc.Object
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasTire` AS ht ON ht.SubjectID = hc.Object
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasTireTread` AS htt ON htt.SubjectID = ht.Object

    UNION

    SELECT r.ID, r.Timestamp, NULL as user, NULL as car, NULL as car, NULL as brand, ht.SubjectID as car, ht.Object as tire, NULL as tire, NULL as tread FROM `Records` AS r
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasTire` AS ht ON r.ID = ht.RecordID
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasCar` AS hc ON hc.Object = ht.SubjectID
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasBrand` AS hb ON hb.SubjectID = hc.Object
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasTireTread` AS htt ON htt.SubjectID = ht.Object

    UNION

    SELECT r.ID, r.Timestamp, NULL as user, NULL as car, NULL as car, NULL as brand, NULL as car, NULL as tire, htt.SubjectID as tire, htt.Object as tread FROM `Records` AS r
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasTireTread` AS htt ON r.ID = htt.RecordID
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasTire` AS ht ON htt.SubjectID = ht.Object
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasCar` AS hc ON hc.Object = ht.SubjectID
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasBrand` AS hb ON hb.SubjectID = hc.Object


Problems
~~~~~~~~

Let's analyze a typical situation that may occur:

.. code-block:: sql

    SELECT ?person ?tireTread WHERE {
        ?person <http://example.com/hasCar>   ?car .
        ?car    <http://example.com/hasBrand> ?brand .
        ?car    <http://example.com/hasTire> ?tire .
        ?tire   <http://example.com/hasTireTread> ?tireTread
    }


The above query asks for which tire treads people use on their cars, please
notice that there is no interest in cars, brands and tires.

This situation, requires the following SQL query:

.. code-block:: sql

    SELECT r.ID, r.Timestamp, hc.SubjectID as user, NULL as tread FROM `Records` AS r
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasCar` AS hc ON r.ID = hc.RecordID
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasBrand` AS hb ON hb.SubjectID = hc.Object
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasTire` AS ht ON ht.SubjectID = hc.Object
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasTireTread` AS htt ON htt.SubjectID = ht.Object

    UNION

    SELECT r.ID, r.Timestamp, NULL as user, htt.Object as tread FROM `Records` AS r
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasTireTread` AS htt ON r.ID = htt.RecordID
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasTire` AS ht ON ht.Object = htt.SubjectID
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasCar` AS hc ON ht.SubjectID = hc.Object
    JOIN `http://rdf.tesladocet.com/ns/person-car.owl#HasBrand` AS hb ON ht.SubjectID = hb.Object

`Note`:
    The number of ``UNION`` is actually given by the number of triples whose subject
    and/or object are in the ``SELECT`` clause of the SPARQL query. This is more
    evident if you also select the ``?tire`` variable in the SPARQL query: the SQL
    query remains the same, it is just necessary to add ``htt.SubjectID as tire`` in
    the ``SELECT`` clause of the SQL query. Thus it would be 2 ``UNION`` for 3
    SPARQL vars and 4 triples, because only 2 triples contains the 3 vars involved.

The problem arise in the result:

==== ====== =====
Time Person Tread
==== ====== =====
1    Luca   NULL
2    NULL   Snow
3    Marco  NULL
4    NULL   Rain
==== ====== =====

Who does use what? The example result has been sorted on purpose to mislead! It
cannot be possible to state which person uses which tire tread, unless revealing
intermediate nodes.

On the other side, the SPARQL query asked only for those information, so it
cannot be added any other column (i.e.: `car` and `tire`). The only solution
may be to merge the rows losing the timing information, which is of paramount
importance for the `History Service`.

==== ====== =====
Time Person Tread
==== ====== =====
1    Luca   Snow
3    Marco  Rain
==== ====== =====

What does it happen at Time = 1? Did Luca buy a new car or did the car have new
tires?
