# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 2021

@author: Stefan Soder

"""

import os
import json
import psycopg2
from configparser import ConfigParser


def obtainconfig(file,section):
    print("Obtaining database configuration.\n")
    parser = ConfigParser()
    parser.read(file)
    dbconfig = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            dbconfig[param[0]] = str(param[1])
        print("Database configuration established.\n")
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, file))
    return dbconfig


def readJson():
    data = []
    jsonDir = r"D:\Databases\json-storage\\"
    jsonList = [str(filename.path) for filename in os.scandir(jsonDir)]
    #print("List of files: {}\n".format(jsonList))
    for file in jsonList:
        with open(file) as f:
            data.append(json.load(f))
        f.close()
    return data
    
def obtainValues(data):
    sqlvalues = []
#   ***Extracting data from dictinary to be passed into SQL query***
    for tweet in data:
        tweetid = tweet['id_str']
        tweettext = tweet['text']
        tweetpolarity = tweet['polarity']
        tweetsubjectivity = tweet['subjectivity']
        tweetdate = tweet['received_at']
        valuetuple = (tweetid,tweettext,tweetpolarity,tweetsubjectivity,tweetdate,)
        sqlvalues.append(valuetuple)
    return sqlvalues

def executeQuery(dbconfig,values):
    querycount = 1
    SQL = """INSERT INTO holdtweets (id,text,polarity,subjectivity,received_at) VALUES (%s,%s,%s,%s,%s) RETURNING id"""
#   ***Connecting to PostgreSQL database to execute data insertion query***
    
    try:
        # Read connection Parameters
        params = dbconfig             
        print("Connecting to the PostgreSQL Database...\n")
        
        # Connect to the PostgreSQL Server
        conn = psycopg2.connect(**params)  
        
        # Create a cursor
        cur = conn.cursor()                
        print("PostgreSQL database version:")
        
        # Execute a statement
        cur.execute('SELECT version()')    
        db_version = cur.fetchone()
        
        # Display PostgreSQL server version, query processing message
        print(db_version,"\n","Executing Insertion query...\n")
        
        # Execute data insertion query
        for data in values:
            print("**Executing insertion query {}**\n".format(querycount))
            print(cur.mogrify(SQL, data),"\n")
            cur.execute(SQL, data)
            print("***Committing transaction {}***\n".format(querycount))
            conn.commit()
            querycount += 1
        # Close the communication w/server
        cur.close()                        
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.\n")

def loadJsonDataPgsql():
    conn = None
    config_file = r'D:\\Scripts\\Projects\\TwitterStream\\config\\database.ini'
    config_section = 'postgresql'
    
#   ***Gather data from local json files***    
    jsondata = readJson()
    sqlvalues = obtainValues(jsondata)
    
#   ***Parse configuration***    
    dbconfig = obtainconfig(config_file,config_section)
#   ***Insert Data to database***    
    executeQuery(dbconfig,sqlvalues)
    
    
    









if __name__ == "__main__":
    loadJsonDataPgsql()
    