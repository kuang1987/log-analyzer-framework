#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import ABCMeta,abstractmethod
from MySQLdb
from elasticsearch import Elasticsearch,helpers

class MysqlJobBase(object):
    __metaclass__ = ABCMeta

    def __init__(self,host=DATABASE_HOST,port=DATABASE_PORT,dbname=DATABASE_NAME,user=USERNAME,password=PASSWORD,elastic_hosts=ELASTIC_HOSTS,charset='utf-8'):
        self.conn = MySQLdb.connect(host=host,
                                    port=port,
                                    user=user,
                                    passwd=password,
                                    db=dbname,
                                    charset=charset)
        self.cursor = self.conn.cursor(cursorclass = MySQLdb.cursors.DictCursor)

        self._result_list = []
        self._elastic_hosts = elastic_hosts

    def pullData(self,sql):
        try:
            result = self.cursor.execute(sql)
            self._result_list = list(self.cursor.fetchall()) 
        except Exception, e:
            raise Exception("[pullData Exception]:" + str(e))

    @abstractmethod
    def processData(self):
        pass

    @abstractmethod
    def actionsGenerator(self):
        pass

    def importElasticSearch(self):
        es = Elasticsearch(hosts=self._elastic_hosts)
        if len(_result_list) == 0:
            raise Exception("[result Empty]")
        (success_num,err_num) = helpers.bulk(es,self.actionsGenerator(self._result_list),True)
        if err_num > 0:
            raise Exception("[Elasticsearch bulk Exception]")

    def __call__(self,sql):
        try:
            self.pullData(sql)
            self.processData()
            self.importElasticSearch()
        except Exception,e:
            raise e

