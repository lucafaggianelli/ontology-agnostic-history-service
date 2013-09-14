Caching system
==============

The *History Service* uses caching in two contexts.

First of all for tables names (random strings) and properties URIs
(see :doc:`names-translation`). Second, for instances and their relative
IDs (see :ref:`sqltable-instances`).

The cache is filled at start-up and is update at every creation of a
property table or instance. The caching system is implemented with a
Python dictionary for each context and it is managed in a LFU (*Least
Frequently Used*) policy, where the least frequently used item is discarded
first: such a policy avoids the cache to grow indefinitely.


Why?
----
The caching system speeds up the read and write operations. Without it
it would necessary to query the DB for the tables names before any operation
and also to translate URIs to IDs performing a ``JOIN`` operation
on the *Instances* table for each instance, complicating a lot the SQL query.
