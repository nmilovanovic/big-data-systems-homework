from cassandra.cluster import Cluster
import uuid

cluster = Cluster(['0.0.0.0'])
session = cluster.connect()
session.execute("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
session.execute('USE test')
session.execute("CREATE TABLE test.count(id UUID PRIMARY KEY,val1 int);");
session.execute("CREATE TABLE test.aggregated(id UUID PRIMARY KEY,val1 float,val2 float, val3 float);")
session.execute("CREATE TABLE test.folded(id UUID PRIMARY KEY,val1 float,val2 float,val3 float,val4 float,val5 float,val6 float);")
session.execute("CREATE TABLE test.combined(id UUID PRIMARY KEY,val1 text, val2 float);")
