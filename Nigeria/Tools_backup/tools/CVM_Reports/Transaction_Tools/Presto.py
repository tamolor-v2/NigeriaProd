import jaydebeapi
import json
import configparser
import argparse
import time
import logging

class presto:

    def __init__(self,host,port,catalog="",schema="",SSL=False,SSLKeyStorePath="",SSLKeyStorePassword="",username="",password="",jarPath="",logger_name="logger"):
        self.jClassName="com.facebook.presto.jdbc.PrestoDriver"
        #"presto-jdbc-350.jar"
        self.jarPath=jarPath
        self.__username=username
        self.__password=password
        #init url
        self.__url=self.__buildUrl(host,port,catalog,schema,SSL,SSLKeyStorePath,SSLKeyStorePassword)
        self.logger_name=logger_name
        

    def __buildUrl(self,host,port,catalog,schema,SSL,SSLKeyStorePath,SSLKeyStorePassword):
        #check params if need secure connection or not
        if(SSL):
            if(catalog):
                if(schema):
                    return "jdbc:presto://{0}:{1}/{2}/{3}?SSL=true&SSLKeyStorePath={4}&SSLKeyStorePassword={5}".format(host,port,catalog,schema,SSLKeyStorePath,SSLKeyStorePassword)
                else:
                    return "jdbc:presto://{0}:{1}/{2}?SSL=true&SSLKeyStorePath={3}&SSLKeyStorePassword={4}".format(host,port,catalog,SSLKeyStorePath,SSLKeyStorePassword)
            else:
                return "jdbc:presto://{0}:{1}?SSL=true&SSLKeyStorePath={2}&SSLKeyStorePassword={4}".format(host,port,SSLKeyStorePath,SSLKeyStorePassword)
        else:
            if(catalog):
                if(schema):
                    return "jdbc:presto://{0}:{1}/{2}/{3}".format(host,port,catalog,schema)
                else:
                    return "jdbc:presto://{0}:{1}/{2}".format(host,port,catalog)
            else:
                return "jdbc:presto://{0}:{1}".format(host,port)
    

    def executeQuery(self,query,escDateKey=False):
        query=query.upper()
        if(not escDateKey):
            if(not ('TBL_DT' in query or 'DATE_KEY' in query)):
                raise ValueError("query does not has a date key.")
        
        try:
            connection=jaydebeapi.connect(jclassname=self.jClassName,url=self.__url,driver_args=[self.__username,self.__password],jars=[self.jarPath])
            cursor=connection.cursor()
            cursor.execute(query)
            results=cursor.fetchall()
            print("Connection established to: ")
        except jaydebeapi.DatabaseError as e:
            raise ValueError(str(e))
        except jaydebeapi.InterfaceError as e:
            raise ValueError(str(e))
        finally:
            if connection:
                cursor.close()
                print("cursor is closed")
                connection.close()
                print("presto connection is closed")
                return results
                

    def executeInsert(self,query,auto_commited=False):
        if(not ('INSERT' in query.upper())):
            raise ValueError("query does not has a Insert command.")

        try:
            connection=jaydebeapi.connect(jclassname=self.jClassName,url=self.__url,driver_args=[self.__username,self.__password],jars=[self.jarPath])
            cursor=connection.cursor()
            cursor.execute(query)
            if(not auto_commited):
                connection.commit()
            rowcount=cursor.rowcount
        except jaydebeapi.DatabaseError as e:
            raise ValueError(str(e))
        except jaydebeapi.InterfaceError as e:
            raise ValueError(str(e))
        finally:
            if connection:
                cursor.close()
                print("cursor is closed")
                connection.close()
                print("presto connection is closed")
                return rowcount



    def executeTransaction(self,statement):
        log = logging.getLogger(self.logger_name)
        try:
            queries = str(statement).split(";")

            transactions=[]
            transaction = []

            for query in queries:
                checkQuery = (query.upper().replace(' ',''))
                if("STARTTRANSACTION" in checkQuery and len(checkQuery) < 17):
                    transaction = []
                if(("STARTTRANSACTION" not in checkQuery) and ("COMMIT" not in checkQuery) and len(checkQuery) > 17):
                    transaction.append(query)
                if("COMMIT" in checkQuery and len(checkQuery) < 7):
                    transactions.append(transaction)
            
            self.start_time = time.time()
            log.info("Count of transction {0}".format(len(transactions)))
            
            transactionCounter=1

            for trans in transactions:
                log.info("Start Transaction Id:{0}".format(transactionCounter))
                log.info("List Transaction Queries: {0}".format(trans))
                connection=jaydebeapi.connect(jclassname=self.jClassName,url=self.__url,driver_args=[self.__username,self.__password],jars=[self.jarPath])
                conn = connection.jconn
                conn.setAutoCommit(False)
                stmt = conn.createStatement()
                start_time = time.time()
                queryCounter=1
                for query in trans:
                    log.info("Run Query {0}: {1}".format(queryCounter,query))
                    stmt.executeUpdate(query)
                    queryCounter = queryCounter+1
                log.info("Execute Transaction Time:{0}".format(time.time() - start_time))
                conn.commit()
                stmt.close()
                conn.close()
                transactionCounter = transactionCounter + 1
        
        except jaydebeapi.ProgrammingError as error:
            conn.rollback()
            raise ValueError(str(error))
        except (Exception, jaydebeapi.DatabaseError) as error:
            conn.rollback()
            raise ValueError(str(error))
        except (Exception, jaydebeapi.InterfaceError) as error:
            conn.rollback()
            raise ValueError(str(error))
        except (Exception, jaydebeapi.Error) as error:
            conn.rollback()
            raise ValueError(str(error))

        finally:
            # closing database connection.
            log.info("Execute Total Time:{0}".format(time.time() - self.start_time))
        
def readJsonFile(path):
    data = json.loads(open(path).read())
    return data


def getArgs():
    parser = argparse.ArgumentParser(prog='parsePlotSens')
    parser.add_argument('-cf','--configPath', help='configPath',required = True)
    parser.add_argument('-q','--query', help='query',required=True)
    parser.add_argument('-i','--insert', help='insert',nargs='?', const='',required = False)
    parser.add_argument('-ed','--escDatekey', help='escDatekey',nargs='?', const='',required = False)
    parser.add_argument('-ep','--extractPath',help="extractPath",required=False)
    parser.add_argument('-e','--email', help='email',nargs='?', const='',required = False)
    return parser.parse_args()

if __name__ == '__main__':
    #get args
    args=getArgs()

    #get configration from file
    configData = readJsonFile(args.configPath)

    host=configData["connection"]["host"]
    port=configData["connection"]["port"]
    catalog=configData["connection"]["catalog"]
    username=configData["connection"]["username"]
    password=configData["connection"]["password"]
    SSLKeyStorePassword=configData["connection"]["SSLKeyStorePassword"]
    SSLKeyStorePath=configData["connection"]["SSLKeyStorePath"]
    SSL=configData["connection"]["SSL"]
    jarPath=configData["connection"]["jarPath"]

    if(SSL):
        print("SSL connection")
        oPresto=presto(host=host,port=port,catalog=catalog,username=username,password=password,SSL=SSL,SSLKeyStorePassword=SSLKeyStorePassword,SSLKeyStorePath=SSLKeyStorePath,jarPath=jarPath)
    else:
        oPresto=presto(host=host,port=port,catalog=catalog,jarPath=jarPath)
        
    #get instance from class
    if(args.insert == None):
        if(args.escDatekey != None):
            result=oPresto.executeQuery(args.query,True)
        else:
            result=oPresto.executeQuery(args.query,False)
    else:
        result=oPresto.executeInsert(args.query)

    #print result
    print(result)
