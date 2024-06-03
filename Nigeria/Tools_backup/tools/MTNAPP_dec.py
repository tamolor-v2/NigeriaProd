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
from csv import reader
import json
from base64 import b64decode
from Cryptodome.Cipher import AES 

def getArgs():
    parser = argparse.ArgumentParser(prog='parsePlotSens')
    parser.add_argument('-i','--inputFile', help='inputFile the encrypted file',required = True)
    parser.add_argument('-o','--outputFile', help='outputFile the decrypted file',required = True)
    parser.add_argument('-k','--keyFile', help='public key file to decrypted the data',required = True)
    return parser.parse_args()


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

def decrypt (key,json_input):
    try:
        b64 = json.loads(json_input)
        json_k = [ 'nonce', 'header', 'ciphertext', 'tag' ]
        jv = {k:b64decode(b64[k]) for k in json_k}
        cipher = AES.new(key, AES.MODE_SIV, nonce=jv['nonce'])
        cipher.update(jv['header'])
        plaintext = cipher.decrypt_and_verify(jv['ciphertext'], jv['tag'])
        print("The message was: " )
        print(str(plaintext).replace('\'','').replace('b',''))
        return str(plaintext).replace('\'','').replace('b','')
    except:
        print("Failed to decrypt this row!")

def main() :
    args=getArgs()
    inFile = args.inputFile
    outFile = args.outputFile
    publicKeyFile = args.keyFile
    with open(publicKeyFile,'r') as pk:
        publicKey = pk.readline()
    publicKey = b'\xa8\x9b\x15\xaf\x0b\x07<x\x8b|y\x0eR\xe8\xc2\x00u\x12?\xedZ1\xab\x0f\xa1\xc7\xef\x1d\xd1\x81\x00:'
    newUsers = []
    temp_out = []
    newfile = []
    guestRecord = []
    final_out = []
    tmpFile = fr"{0}tmp.csv".format(getBaseDir(args.inputFile))
    tmpBase64File = fr"{0}tmp2.txt".format(getBaseDir(args.inputFile))
    with open(inFile,'r') as read_csv:
        csv_reader = reader(read_csv)
        csv_read = []
        for row in csv_reader:
            if (((str(row).split(';'))[0][2:] != 'user_id') and (str(row).split(';'))[0][2:] != 'unidentified:guest'):
                newUsers.append((str(row).split(';'))[0][2:])
                tmp = str(row).split(';')
                csv_read.append(';'.join(tmp))
            if ((str(row).split(';'))[0][2:] == 'unidentified:guest'):
                guestRecord.append("0" + ";" + str(row))

        for users in newUsers:
            base64File = open(tmpFile,"w")
            base64File.write(str(users)+"\n")
            base64File.close()
            runCmd("base64 -d {0} > {1}".format(tmpFile,tmpBase64File))
            with open(tmpBase64File,'r') as b:
                json_input = b.readline()
            #print(json_input)
            outpt_dec = decrypt(publicKey,json_input)
            temp_out.append(outpt_dec)
            #print(outpt_dec)
            runCmd("rm {0}".format(tmpBase64File))
            runCmd("rm {0}".format(tmpFile))
        for nm in range (0,len(csv_read)):
            print("The message was: " ) #print('-------')
            print(str(temp_out[nm]))
            newfile.append(str(temp_out[nm]) + ";" + str(csv_read[nm][2:-1]))
    with open(outFile,'w+') as write_csv:
        for i in newfile:
            write_csv.write(str(i).replace('\'','') + "\n")
        for j in guestRecord:
            write_csv.write(str(j).replace('\'','').replace('[','').replace(']','') + "\n")

if __name__ == "__main__" :
    main()
