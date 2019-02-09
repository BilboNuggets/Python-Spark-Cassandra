from cassandra.cluster import Cluster, log
from cassandra import DriverException
from symbol import parameters
from GeneralHandler import GeneralHandler

class CassandraHandler:
    
    __cluster = Cluster()  # The cluster to which Cassandra is connected
    __session = __cluster.connect()  # The session which allows performing queries on Cassandra's keyspace
    __generalHandler = GeneralHandler()
    
    # Description: Initialize the keyspace on which queries will be performed
    # Params: keyspace -> Name of the keyspace
    def __init__ (self, keyspace=""):
        
        if(len(keyspace) > 0):
            try:
                self.__session.set_keyspace(keyspace)
            except:
                self.__generalHandler.printException()
                pass
        
    # Description: create a keyspace with default params if it didn't exist
    # Params: keyspace -> Name of the keyspace
    #         replication_factor -> How many nodes (replicas) data will reside into
    #         strategy -> The network topology of nodes 
    def createKeyspace (self, keyspace, replication_factor=2, strategy="SimpleStrategy"):
        try:
            self.__session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': '%s', 'replication_factor': '%s' }
        """ % (keyspace , strategy , replication_factor))
            
            self.__session.set_keyspace(keyspace)
            print ("Keyspace created successfully.")
            return True;
                                    
        except:
            self.__generalHandler.printException()
            pass
    
    # Description: Drop keyspace if it exists from cassandra
    # Params: keyspace -> Name of the keyspace
    def deleteKeyspace (self, keyspace):
        try:
            self.__session.execute(" DROP KEYSPACE %s" % keyspace)
            print ("Keyspace dropped successfully.")
            return True
        except:
            self.__generalHandler.printException()
            pass
    
    # Description: Perform a single query on the currently active keyspace 
    # Params: queryStmt ->  The query that will be performed
    #         parameters -> A list of bind parameters
    # EX : .query("INSERT INTO CHANNELS (PK, NAME, YOUTUBE_CHANNEL, YOUTUBE_CHANNEL_ID) VALUES (%s, %s, %s, %s)",
    #                                       [nextChannelPk, channelName, youtubeChannel, youtubeChannelId])
    def query (self, queryStmt, parameters=""):
        
        try:
            rows = self.__session.execute(queryStmt, parameters)
            
            if ("insert" in queryStmt.lower()) | ("update" in queryStmt.lower()):
                return 1
            
            else:
                return rows.current_rows
            
        except:
            self.__generalHandler.printException()
            pass
    
    
    # Description: Perform a batch (list) of queries on the currently active keyspace 
    # Params: queryList -> List of queries containing multiple DDL statements  
    def batchInsert (self, queryList):
        
        try:
            
            BATCH_STMT = 'BEGIN BATCH '
            
            for query in queryList:
                
                BATCH_STMT += query
            
            BATCH_STMT += ' APPLY BATCH;'
                    
            prep_batch = self.__session.prepare(BATCH_STMT)
            self.__session.execute(BATCH_STMT)            
            
            return 1
        except:
            self.__generalHandler.printException()
            pass
    
    # Description: Get the next primary key value of a table using nextpk table (default) 
    # Params: table -> Name of the table
    def getNextPk (self, table):
        try:
            nextPk = self.query("select pk from nextpk where tablename = %s", [table])
            
            if(len(nextPk) > 0):
                return nextPk[0][0]
            else:
                return 1;
            
        except:
            self.__generalHandler.printException()
            pass
    
    # Description: Get the next primary key value of a table by getting the next to max value 
    # Params: table -> Name of the table
    def getNextPKByMax (self, table):
        
        try:
            retVal = self.Query("SELECT MAX(PK) FROM " + table)[0][0]
            if (retVal > 0):
                return int(retVal) + 1
            else:
                return 1
            
        except:
            self.__generalHandler.printException()
            pass
        
