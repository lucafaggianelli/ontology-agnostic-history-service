# Ontology Constants

ns="http://mml.arces.unibo.it/Ontology1243431093.owl#"

HISTORY=ns+"HISTORY"

# History Request
HISTORY_INPUT=ns+"HISTORYINPUT"
HAS_HISTORY_INPUT=ns+"HasHistoryInput"

# History Read Request
HISTORY_READ = ns + "HistoryReadRequest"
HAS_HISTORY_READ = ns + "HasHistoryReadRequest"
HAS_HISTORY_READ_RESPONSE = ns + "HasHistoryReadResponse"

# Attach a SPARQL query to both read and input
HAS_SPARQL = ns+"HasHistorySPARQLQuery"


# History Backend DB
HAS_HISTORY_DB = ns + "HasHistoryDB"
HISTORY_DB = ns + "HistoryDB"

HAS_RDBMS = ns + "HasRDBMS" # MySQL
HAS_DB_HOST = ns + "HasDBHost" # localhost
HAS_DB_NAME = ns + "HasDBName" # history
HAS_DB_USER = ns + "HasDBUser" # luca
HAS_DB_PASS = ns + "HasDBPass" # 12341324


RECORD=ns+"Record"
HAS_RECORD=ns+"HasRecord"
HAS_VALUE=ns+"HasValue"
HAS_DATA=ns+"HasData"

DATA_RECORD=ns+"DATARECORD"