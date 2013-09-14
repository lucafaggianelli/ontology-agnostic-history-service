Behind the Scenes
=================

The `History Service` working principle may be roughly summarized as
archiving RDF triples in a DB not loosing their semantics.

The History Service uses subscriptions to accept History Requests and
History Read Requests and these are the only to way to communicate with it.




.. toctree::
    :maxdepth: 2

    behind-scene/SQL
    behind-scene/names-translation
    behind-scene/caching
    behind-scene/decanter
    behind-scene/db-adapters
