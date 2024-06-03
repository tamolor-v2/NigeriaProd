from datetime import datetime, timedelta
import os,sys,fileinput
import subprocess as s
import time
import argparse
from shutil import copyfile
import threading
from collections import deque
import logging

import pandas as pd
import base64 as bs
from csv import reader

def getArgs():
    parser = argparse.ArgumentParser(prog='parsePlotSens')
    parser.add_argument('-i','--inputFile', help='inputFile the encrypted file',required = True)
    parser.add_argument('-o','--outputFile', help='outputFile the decrypted file',required = True)
    parser.add_argument('-k','--keyFile', help='public key file to decrypted the data',required = True)
    return parser.parse_args()

def addHeader(file):
    df = pd.read_csv(file, header=None)
    df.to_csv(file, header=["user_id"], index=False)

def formatFile(file):
   for line in fileinput.input(file, inplace=1):
       line = line.replace('+', '\n+')
       sys.stdout.write(line[1:])

def runCmd(command):
    proc = s.Popen(command, shell=True, stdout=s.PIPE, stderr=s.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, str(s_output).replace("b'","").replace("\\n'",""), s_err

def getBaseDir(fileName):
    if (fileName.find('.')!=-1):
        lengthOfFile = fileName.rfind('/')
        basePath = fileName[:lengthOfFile+1]
        return basePath
    else:
        return fileName


def mergeFiles(inFile,outFile):
    inputFile = pd.read_csv(inFile)
    outputFile= pd.read_csv(outFile)
    outputFile.insert(1,"encrypted_user_id",inputFile["user_id"],True)
    outputFile.insert(2,"date",inputFile["optin_date"],True)
    
    outputFile.to_csv(r'yahyaFile.txt',header=None,index=None,sep=';',mode='a')
    truncFile = open(outFile,"w")
    truncFile.truncate()
    truncFile.close()
    
def main() :
    args=getArgs()
    inFile = args.inputFile
    outFile = args.outputFile
    publicKeyFile = args.keyFile
    tmpFile = fr"{0}tmp.csv".format(getBaseDir(args.inputFile))
    tmpBase64File = fr"{0}tmp2.txt".format(getBaseDir(args.inputFile))
    with open(inFile,'r') as read_csv:
        csv_reader = reader(read_csv)
        for row in csv_reader:
            print(row)
            if(len(row)>1 and 'user_id' not in row):
                encryptedNum = row[0]
                if len(encryptedNum) %4:
                    encryptedNum += '=' * (4 - len(encryptedNum) % 4)
                base64File = open(tmpFile,"w")
                base64File.write(str(encryptedNum)+"\n")
                base64File.close()
                a = runCmd("base64 -d {0} > {1}".format(tmpFile,tmpBase64File))
                print(a)
#                runCmd("openssl rsautl -decrypt -inkey {0} -in {1}  >> {2}".format(publicKeyFile,tmpBase64File,outFile))
#                runCmd("rm {0}".format(tmpBase64File))
#                runCmd("rm {0}".format(tmpFile))
    formatFile(outFile)
    addHeader(rf"{outFile}")
    mergeFiles(inFile,outFile)
    runCmd("cat yahyaFile.txt > {0}".format(outFile))
    runCmd("rm {0}".format(r'yahyaFile.txt'))
    runCmd("chmod 777 {0}".format(outFile))
    #runCmd("beeline --showHeader=false --outputformat=tsv2 -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e 'LOAD DATA LOCAL INPATH '{0}' INTO TABLE kamanja_test.FACEBOOK_DATA;'".format(outFile))
#    insertToTable(r"{outFile}")

if __name__ == "__main__" :
    main()

