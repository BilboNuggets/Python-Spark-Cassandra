from SparkHandler import SparkHandler
from CassandraHandler import CassandraHandler

cassandraHandler = CassandraHandler("test")
sparkHandler = SparkHandler(keyspace="test", appName="test")
