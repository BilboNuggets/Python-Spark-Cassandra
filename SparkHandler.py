from pyspark import *
from pyspark.sql import *  # The entry point to programming Spark with the Dataset and DataFrame API.
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType
import json
from GeneralHandler import GeneralHandler
 
# Spark and Cassandra database handler for connecting, querying ... etc  
 
class SparkHandler:
    __generalHandler = GeneralHandler()
     
    __spark = ""  # Spark is a SparkSession
    __conf = ""  # Spark configuration parameters
    __sparkContext = ""  # Connection to Spark cluster. 
    __sqlContext = ""  # Working on data as structured 
    __dataFrames = []  # list of data frames
    __currKeyspace = ""  # Current worked on Cassandra keyspace
     
    # Description: Initialization of spark worker
    # Params: keyspace -> Name of Cassandra's keyspace
    #         appName -> The name of spark worker name
    #         sparkMaster -> Spark cluster
    def __init__(self, keyspace, appName="general", sparkMaster="local[*]"):
        
        try:
            # A SparkSession can be used create DataFrame, register DataFrame as tables, execute SQL over tables, cache tables, and read parquet files.
            # With a SparkSession, applications can create DataFrames from an existing RDD, from a Hive table, or from Spark data sources.
            # getOrCreate(): Gets an existing SparkSession or, if there is no existing one, creates a new one based on the options set in this builder.
            self.__spark = SparkSession.builder.master(sparkMaster).appName(appName).getOrCreate()
             
            # A SparkContext: represents the connection to a Spark cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
            self.__sparkContext = self.__spark.sparkContext
             
            # pyspark.sql.SQLContext(sparkContext): The entry point for working with structured data (rows and columns) in Spark, in Spark 1.x.
            # A SQLContext can be used create DataFrame, register DataFrame as tables, execute SQL over tables, cache tables, and read parquet files.
            self.__sqlContext = SQLContext(self.__sparkContext)
             
            # Set current active Cassandra keyspace
            self.__currKeyspace = keyspace.lower()
            
        except Exception as e:
            self.__generalHandler.printException()
            pass
             
    # Description: Used to create select queries on multiple tables
    # Params: tables -> Name of the tables
    #         query -> The query to be executed
    #         returnFormat -> Format of returned data (json, flatMap, list of rows ... etc)
    def selectQuery(self, tables, query, returnForamt="default"):
        
        try:
            
            for table in tables:
                
                # convert to lowercase because of Cassandra's convention
                table = table.lower()
                
                # Create a dataframe for each table to retrieve data
                current_dataframe = self.__sqlContext.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=self.__currKeyspace).load()
                
                # Create a temp view for each table to perform the query on 
                current_dataframe.createOrReplaceTempView(table)
            
            # check return format of retrieved data whether it's going to be json, flatMap or list of rows (default)
            if returnForamt == "json":
                retVal = self.__sqlContext.sql(query).toJSON(False).collect()
            elif returnForamt == "flatMap":
                retVal = self.__sqlContext.sql(query).rdd.flatMap(lambda x:x).collect()
            else:
                retVal = self.__sqlContext.sql(query).collect()
            
            # Clear temp view of each table to free up memory
            self.__spark.catalog.clearCache()
                
            return retVal
        
        except Exception as e:
            self.__generalHandler.printException()
            pass
        
        return 0
    
    # Description: Insert a list of data into the current table inside the current keyspace
    # Params: table -> Name of the table into which data will be saved
    #         data -> The list of data that will be inserted
    #         saveMode -> The type of saving (append, overwrite ... etc)
    def tableInsert(self, table, data, saveMode="append"):
        
        try:
        
            dataFrame = self.__sqlContext.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=self.__currKeyspace).load()
    
            # A list of options containing the current table and keyspace
            # and a special parameter (Level of consistency) which indicates that at least one node in cassandra cluster
            # data inserted into
            load_options = { "table": table, "keyspace": self.__currKeyspace, "spark.cassandra.output.consistency.level": "ONE"}
             
            # Create a temp dataframe for each row to be append to the original dataframe
            newRow = self.__sqlContext.createDataFrame(data)
            
            # Union the newly added data to the current (original) dataframe
            dataFrame = dataFrame.union(newRow)
             
            # Append (save) data to cassandra database 
            dataFrame.write.format("org.apache.spark.sql.cassandra").mode(saveMode).options(**load_options).save()
            
            # Clear temp views to free up memory
            self.__spark.catalog.clearCache()
            
            return 1
        
        except Exception as e:
            self.__generalHandler.printException()
            pass
        
        return 0
    
    # Description: Delete row(s) from a table
    # Params: table -> Name of the table from which data will be deleted
    #         whereCond -> The condition applied on deleted data    
    def tableDelete(self, table, whereCond):
        
        try:
        
            # convert to lowercase because of Cassandra's convention
            table = table.lower()
            
            # Select all data from this table except that are not meeting the were condition
            data = self.selectQuery([table], "SELECT * FROM " + table + " WHERE PK NOT IN (SELECT PK FROM " + table + " WHERE " + whereCond + ")")
            
            # Overwrite this data in the table
            self.tableInsert(table, data, "overwrite")
            
            return 1
            
        except Exception as e:
            self.__generalHandler.printException()
            pass
        
        return 0
     
    # Description: Get the next PK of a table
    # Params: table -> name of the table
    def getNextPK (self, table):
        
        # convert to lowercase because of Cassandra's convention
        table = table.lower()
        
        try:
            # PK is returned in format of [Row(pk_no)]
            retVal = self.selectQuery([table], "SELECT MAX(PK) FROM " + table)[0][0]
             
            # If the table has data return the next pk
            if (retVal > 0):
                return int(retVal) + 1
             
            # if the table is empty return 1
            else:
                return 1
            
        except Exception as e:
            self.__generalHandler.printException()
            pass
        
        return 0
     
    # Description: Performs a query on a list of data (in format of spark flatMap) 
    # Params: dataTable -> name main table in data
    #         data -> the data on which query will be performed
    #         query -> query to be executed on data as a table
    def registerAsDataframe(self, dataTable, data):
        try:
            # Convert data to spark RDD to be able to convert it to a table (dataframe)
            dataRDD = self.__sparkContext.parallelize(data)
                    
            # Generate a dataframe of this RDD
            dataDataframe = self.__sqlContext.read.json(dataRDD)
                    
            # Create a temp view for data table to perform the query on 
            dataDataframe.createOrReplaceTempView(dataTable)
                        
            return 1
        
        except Exception as e:
            self.__generalHandler.printException()
            pass
        
        return 0
