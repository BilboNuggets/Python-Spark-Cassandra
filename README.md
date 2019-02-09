# Python-Spark-Cassandra
This is a sample code of spark & cassandra handlers using python on Ubuntu 16.04 & Eclipse IDE.
### Installation instructions
# Important note: Latest versions may differ, please check compatability of used components before installing.
For installing python 3 & cassandra please refer to these links
### Python https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04
### Cassandra http://cassandra.apache.org/download/

### Spark
1-Download the latest version of spark through their website.
2-Install Scala 2.11 (because of the pyspark-cassandra-connecter only supports 2.11 and below versions).<br>
3-Spark installation https://www.santoshsrinivas.com/installing-apache-spark-on-ubuntu-16-04/ <br>
4-Install pyspark and integrate it with eclipse: http://stackoverflow.com/questions/33326749/pyspark-in-eclipse-using-pydev
install setup.py in $SPARK_HOME/python <br>
5-Install spark-cassandra-connector (https://github.com/datastax/spark-cassandra-connector):
```
-Look up the Version Compatibility table
-For packages src, run the commands in "How-to" compatible with your versions: https://spark-packages.org/package/datastax/spark-cassandra-connector (Run both pyspark and spark-submit commands)
``` 
After running the commands, create $SPARK_HOME/conf/spark.defaults.conf by running
```
sudo cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
```
-Then add this line at the end (whatever version the connector was installed)
```
spark.jars.packages   datastax:spark-cassandra-connector:2.0.1-s_2.11
```
