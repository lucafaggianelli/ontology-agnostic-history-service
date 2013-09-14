.. _naming-system:

Naming System
=============

The architecture of the *History Service* requires a DB with tables whose name
is the URI of a property (i.e.: 'http://rdf.tesladocet.com/ns/person-car.owl#HasCar').
Unfortunately RDBMS allow only 32 to 64 chars for tables names and a URI
may easily overflow this size, so the adopted solution is to use a random string
of length 32 with the leading character to be a letter and the successive 31
alphanumeric chars.

The method :func:`DatabaseWriter._random_id() <DatabaseConnector.DatabaseWriter.DatabaseWriter._random_id>`
returns such a random string.

The relation between the real random name of the table and the name the table
should have, is saved inside the :ref:`sqltable-namesdictionary`. This table is
entirely fetched when the *History Service* daemon starts and its content is
is cached for speed purposes. For more information, please read the :doc:`caching`
page.
