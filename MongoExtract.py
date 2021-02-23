# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 2021

@author: Stefan Soder

"""

# MongoDB initialization command: 
# "C:\Program Files\MongoDB\Server\4.4\bin\mongod.exe" --dbpath "D:\Databases\mongodb"
# Mongo Shell command:
# "C:\Program Files\MongoDB\Server\4.4\bin\mongo.exe" "mongodb://127.0.0.1:27017/?authSource=admin"

import json
import pymongo
import os
from pymongo import MongoClient
from configparser import ConfigParser
from urllib.parse import quote_plus
from pymongo import MongoClient 
from pymongo.errors import ConnectionFailure




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

def mongologin(config):
    mongo_uri = 'mongodb://%s:%s@%s?authSource=%s' % (quote_plus(config['mongo_user']),quote_plus(config['mongo_pass']),config['mongo_host'],config['mongo_auth'])
    mon_client = MongoClient(host=mongo_uri)
    mon_db = mon_client.tweetdb
    mon_col = mon_db.tweets
    try:
        print(mon_db.list_collection_names(),"\n")
        print("Connected to MongoDB Client, ready for data.\n")
    except ConnectionFailure: 
        print("Sorry, connection failed!\n")
    return (mon_client)

def mongoToJSON():
    config_file = r'D:\\Scripts\\Projects\\TwitterStream\\config\\database.ini'
    config_section = 'mongodb'
    jsonDir = r'D:\\Databases\\json-storage\\'
    fileCount = 0
    
    print("Accessing MongoDB.\n")
    dbconfig = obtainconfig(config_file,config_section)
    mongo = mongologin(dbconfig)
    db = mongo.tweetdb
    
    print("Beginning document reads...\n")
    for data in db.tweets.find({}).sort('received_at',pymongo.ASCENDING).limit(100):
        del data["_id"]
        tweetfile = jsonDir+data.get("id_str")+".json"
        #print(tweetfile,"\n")
        with open(tweetfile, "w+") as file:
            json.dump(data, file)
            #print("JSON document created succesfully.\n")
        file.close()
        fileCount += 1
    return "Processed {} records from MongoDB. Located in {}".format(fileCount,jsonDir)
    

#******************************************************
#
#           Functional Testing Below
#
#******************************************************

if __name__ == "__main__":
    
    returnstring = mongoToJSON()
    print("\n********************\n{}\n********************\n".format(returnstring))