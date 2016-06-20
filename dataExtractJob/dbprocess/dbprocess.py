#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
from pymongo import MongoClient
from config import *


MONGO_BATCH_LIMIT = 5000

class dbprocess():
    def __init__(self,host=DATABASE_HOST,port=DATABASE_PORT,dbname=DATABASE_NAME,user=USERNAME,password=PASSWORD,charset='utf8'):
        self.conn = MySQLdb.connect(host=host,
                                    port=port,
				    user=user,
				    passwd=password,
				    db=dbname,
				    charset=charset)
        self.cursor = self.conn.cursor(cursorclass = MySQLdb.cursors.DictCursor) 

    def execute(self,sql,myclass=None):
        result = self.cursor.execute(sql)
        return list(self.cursor.fetchall())
        #for row in self.cursor.fetchall():
        #    for r in row:
        #        print r

class mongodbprocess():
    def __init__(self,host=MONGODB_HOST,port=MONGODB_PORT,dbname=MONGODB_DB_NAME,user=MONGODB_USERNAME,password=MONGODB_PASSWORD):
        auth = ''
        if user != '':
            auth = user + ':' + password + '@'
        self.uri = 'mongodb://' + auth + host + ':' + str(port) + '/' + dbname
        self.c = MongoClient(host = self.uri)
        self.db = self.c[dbname]

    def insert(self,c_name,list):
        #if True:
        try:
            collection = self.db.get_collection(c_name)
        #if len(list) > MONGO_BATCH_LIMIT:
        #    times = len(list)/MONGO_BATCH_LIMIT
        #    index = 0
        #    for i in range(0,times):
        #        result = collection.insert_many(list[i*MONGO_BATCH_LIMIT:(i+1)*MONGO_BATCH_LIMIT])
        #        index += 1
        #    result = collection.insert_many(list[index*MONGO_BATCH_LIMIT:])
        #else:
        #    result = collection.insert_many(list)
            for entry in list:
                result = collection.insert(entry)
        except:
            raise Exception("MongoDB -- insert %s error!"%c_name)

    def drop(self,c_name):
        try:
            self.db.drop_collection(c_name)
        except:
            raise Exception("MongoDB -- drop %s error!"%c_name)

    def createIndex(self,c_name,index_field):
        try:
           collection = self.db.get_collection(c_name)
           collection.create_index(index_field,unique=True)
        except:
            raise Exception("MongoDB -- create index for %s error!"%c_name)

    def mapReduce(self,c_name,map,reduce,**kwargs):
        try:
           collection = self.db.get_collection(c_name)
           result = collection.inline_map_reduce(map,reduce,**kwargs)
           return result
        except:
           raise Exception("MongoDB -- mapreduce for %s error!"%c_name)

if __name__ == '__main__':
    #db = dbprocess(dbname='oper_db_test')
    #print db.execute("select * from authorization where mallcode = 'Auto5Plus2_01'")
    #print db.execute("desc cargo_info")

    mongodb = mongodbprocess()
    #mongodb.drop('mytest')
    #mongodb.createIndex('mytest','a')
    list = []

    for i in range(0,103234):
        list.append({'a':i})

    if len(list) > MONGO_BATCH_LIMIT:
        times = len(list)/MONGO_BATCH_LIMIT
        index = 0
        for i in range(0,times):
            print str(i*MONGO_BATCH_LIMIT) + "  ---  " + str((i+1)*MONGO_BATCH_LIMIT)
            index += 1

    mongodb.insert('mytest',list)
