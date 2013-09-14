Reference Manual
================

.. toctree::
    :maxdepth: 2

    History Service Daemon <reference/HistoryService>
    History Service Client <reference/HistoryClient>

If you are going to develop something for this project, you are welcome
to maintain this documentation. These pages are written with
`reStructuredText (rst) <http://docutils.sourceforge.net/rst.html>`_.
To compile in HTML run ``make html`` inside the ``doc`` folder, which contains
all the sources but the index page which is directly written in HTML and is in 
the ``doc/_templates/`` folder. HTML output is in ``doc/_build/html``, copy its
content to your webserver.

To start developing, please clone the Git repository on your local PC:

.. code-block:: shell

    git clone https://github.com/lucafaggianelli/ontology-agnostic-history-service
