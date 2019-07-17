# Create Cassandra Cluster
# initiating a connection to the Cassandra Cluster on
# (127.0.0.1)

from cassandra.cluster import Cluster
try:
    cluster = Cluster(['127.0.0.1'])
    # Initiating a session
    session = cluster.connect()
except Exception as e:
    print(e)
    
#Create udacity Keyspace 

try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION =
    {'class' : 'SimpleStrategy' , 'replication_factor' : 1}""")
    
except Exception as e:
    print(e)
    
# Setting KEYSPACE to udaicty keyspace 
try:
    session.set_keyspace('udacity')
    
except Exception as e:
    print(e)
