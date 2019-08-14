import psycopg2
import sys
import json
import os


def load_csv(csvFile, tableName, dbName, host, port, user, password):
    
    try:
        
        conn = psycopg2.connect(dbname=dbName,
                                host=host,
                                port=port,
                                user=user,
                                password=password)
        
        print "Connecting to Database"
        
        cur = conn.cursor()
        file = open(csvFile, "r")
        
        # Truncate table
        cur.execute("Truncate {} Cascade;".format(tableName))
        print "Truncated {}".format(tableName)
        
        # Load table with header
        cur.copy_expert("copy {} from STDIN CSV HEADER QUOTE '\"'".format(tableName), file)
        cur.execute("commit;")
        print "Loaded data into {}".format(tableName)
        
        conn.close()
        print "DB connection closed."

    except Exception as e:
        
        print "Error: {}".format(str(e))
        sys.exit(1)


# Function call
with open(os.path.join(os.path.dirname(__file__), 'config.json')) as config_file:
    cfg = json.load(config_file)

csvFile = cfg['csvFile']
tableName = cfg['tableName']
dbName = cfg['dbName']
host = cfg['host']
port = cfg['port']
user = cfg['user']
password = cfg['password']

load_csv(csvFile, tableName, dbName, host, port, user, password)
