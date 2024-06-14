import psycopg2
import json
import configparser
import argparse
# import time library
import time

class postgres:
    def __init__(self,host,port,database,username,password):        
        #set connect config from file
        self.__host=host
        self.__port=port
        self.__database=database
        self.__username=username
        self.__password=password
    
    def check_poll_status():
        if conn.poll() == extensions.POLL_OK:
            print ("POLL: POLL_OK")
        if conn.poll() == extensions.POLL_READ:
            print ("POLL: POLL_READ")
        if conn.poll() == extensions.POLL_WRITE:
            print ("POLL: POLL_WRITE")
        return conn.poll()

    def get_transaction_status():
        # print the connection status
        print ("\nconn.status:", conn.status)
        # evaluate the status for the PostgreSQL connection
        if conn.status == extensions.STATUS_READY:
            print ("psycopg2 status #1: Connection is ready for a transaction.")
        elif conn.status == extensions.STATUS_BEGIN:
            print ("psycopg2 status #2: An open transaction is in process.")
        elif conn.status == extensions.STATUS_IN_TRANSACTION:
            print ("psycopg2 status #3: An exception has occured.")
            print ("Use tpc_commit() or tpc_rollback() to end transaction")
        elif conn.status == extensions.STATUS_PREPARED:
            print ("psycopg2 status #4:A transcation is in the 2nd phase of the process.")
        return conn.status

    def executeQuery(self,query,escDateKey=False,withColumns=False):
        #check date key 
        uquery=query.upper()
        if(not escDateKey):
            if(not ('TBL_DT' in uquery or 'DATE_KEY' in uquery)):
                raise ValueError("query does not has a date key.")

        try:
            #establishing the connection
            connection = psycopg2.connect(database=self.__database, user=self.__username, password=self.__password, host=self.__host, port=self.__port)
            
            #Creating a cursor object using the cursor() method
            cursor = connection.cursor()

            #Executing an MYSQL function using the execute() method
            cursor.execute(query)

            # Fetch a single row using fetchone() method.
            data = cursor.fetchall()
            
            col_names = []
            if(withColumns):
                for elt in cursor.description:
                    col_names.append(elt[0])

        except (Exception, psycopg2.Error) as error:
            raise ValueError(str(error))
        finally:
            # closing database connection.
            if connection:
                cursor.close()
                print("cursor is closed")
                connection.close()
                print("PostgreSQL connection is closed")
                return data,col_names
    
    
    def executeInsert(self,query):
        #check date key 
        
        if(not ('INSERT' in query.upper())):
            raise ValueError("query does not has a Insert command.")

        try:
            #establishing the connection
            connection = psycopg2.connect(database=self.__database, user=self.__username, password=self.__password, host=self.__host, port=self.__port)
            
            #Creating a cursor object using the cursor() method
            cursor = connection.cursor()

            #Executing an MYSQL function using the execute() method
            cursor.execute(query)
            connection.commit()
            count=cursor.rowcount

        except (Exception, psycopg2.Error) as error:
            raise ValueError(str(error))
        
        finally:
            # closing database connection.
            if connection:
                cursor.close()
                print("cursor is closed")
                connection.close()
                print("PostgreSQL connection is closed")
                return count
    
    def executeCreate(self,query):
        if(not ('CREATE' in query.upper())):
            raise ValueError("query does not has a create command.")
        
        try:
            #establishing the connection
            connection = psycopg2.connect(database=self.__database, user=self.__username, password=self.__password, host=self.__host, port=self.__port)
            
            #Creating a cursor object using the cursor() method
            cursor = connection.cursor()

            #Executing an MYSQL function using the execute() method
            cursor.execute(query)
            connection.commit()
            count=cursor.rowcount

        except (Exception, psycopg2.DatabaseError) as error:
            raise ValueError(str(error))
        
        finally:
            # closing database connection.
            if connection:
                cursor.close()
                print("cursor is closed")
                connection.close()
                print("PostgreSQL connection is closed")
        
    def executeTransaction(self,query):
        try:
            #establishing the connection
            
            connection = psycopg2.connect(database=self.__database, user=self.__username, password=self.__password, host=self.__host, port=self.__port)

            #set session as manual commit
            connection.set_session(autocommit=False)

            #Creating a cursor object using the cursor() method
            cursor = connection.cursor()

            self.start_time = time.time()
            #Executing an MYSQL function using the execute() method
            cursor.execute(query)
            
            #commit transaction
            connection.commit()

        except psycopg2.ProgrammingError as error:
            connection.rollback()
            raise ValueError(str(error))
        except (Exception, psycopg2.DatabaseError) as error:
            connection.rollback()
            raise ValueError(str(error))
        except (Exception, psycopg2.InterfaceError) as error:
            connection.rollback()
            raise ValueError(str(error))
        except (Exception, psycopg2.Error) as error:
            connection.rollback()
            raise ValueError(str(error))

        finally:
            # closing database connection.
            if connection:
                print ("Execute Time:", time.time() - self.start_time)
                cursor.close()
                connection.close()
                print("Excecution has been finished...")




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

    args=getArgs()
    configData = readJsonFile(args.configPath)

    host=configData["connection"]["host"]
    port=configData["connection"]["port"]
    database=configData["connection"]["database"]
    username=configData["connection"]["username"]
    password=configData["connection"]["password"]

    #get instance from class
    oPostgres=postgres(host,port,database,username,password)
    
    #execute select query
    if(args.insert == None):
        if(args.escDatekey != None):
            result=oPostgres.executeQuery(args.query,True)
        else:
            result=oPostgres.executeQuery(args.query,False)
    else:
        result=oPostgres.executeInsert(args.query)

    #print result
    print(result)
