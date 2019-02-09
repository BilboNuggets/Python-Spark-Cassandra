from SparkHandler import SparkHandler
from CassandraHandler import CassandraHandler

# port number 
PORT_NUMBER = 8080
 
cassandraHandler = CassandraHandler("test")
sparkHandler = SparkHandler(keyspace="test", appName="test")
