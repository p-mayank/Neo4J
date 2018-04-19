# Neo4J
Implemented twitter database on Neo4J.

* Please run the loadq file inside the dataset folder to create the initial tables. DB used is : 'twitter_db'

* Tables schema is detailed in the file: 'loadq.py' under 'load_data' function

* Flask files include the 'templates' folder along with the executable 'flask_run.py' file

* External Libraries used are:
-'json' (Read/Write JSON files)
-'os' (Iterating over files)
-'py2neo' (Driver to connect to the neo4j db)
-'flask' (Setting up a web-interface.)
-'datetime' (Utility to parse date fields in Python)

// Recommended version of Python is 3.5;
