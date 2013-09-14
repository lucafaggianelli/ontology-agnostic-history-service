.. _decanter:

Decanter
========

The `Decanter` is an exotic name for a buffer. The History Service never
writes directly to the database: it does write data to a buffer then every
a fixed and settable amount of time (few seconds generally) data is written
from this buffer to the historical database.

The Decanter is needed to avoid data duplication and the overlap of two or
more History Requests.
When the History Service writes to the Decanter, it checks that the triples
do not already exist, if they do, they are not written. Thus one is sure
that there is no data duplication (identical triples) for intervals of time
equal to the time interval with which information is fetched from the Decanter
and `poured` into the database.

On the other hand side, the phenomenon of requests overlap appears when
clients issue requests with SPARQL queries that share common parts, that is,
the graphs associated to the SPARQL queries have at least one node in common.
When this occurs, there are more subscriptions that inject the same data
in the historical repository, but using the Decanter this duplicated data is
filtered.
The overlap problem may be solved in other ways like manipulating the SPARQL
queries when the requests are issued, but this involves a heavy computation.

If you are curios about the name, now it should be clear that the `Decanter`
functionality in the History Service is similar to the one of the real wine
Decanter where one pours the wine and let it settle for some time before pouring
it in the glass.
