#!/usr/bin/env python

import os,sys,re
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'../'))

from abc import ABCMeta,abstractmethod
from elasticsearch import Elasticsearch,helpers
import MySQLdb
from pymongo import MongoClient,DESCENDING
import types
import textwrap
import config
from utils.dateUtils import *
import datetime

CONFIG_TEMPLATE = {'source':{
                            'hosts':True,
                            'username':False,
                            'password':False,
                            'dbname':True
                            },
                    'query':{
                            'querydsl': True,
                            'params': False,
                            },
                    'scrpit': False,
                    'target':{
                             'hosts': True,
                             'index': True,
                             'type': True,
                             'document_id':False
                             }
                  }

class DataJobTemplate(object):
    __metaclass__ = ABCMeta

    def __init__(self,options):
        super(DataJobTemplate,self).__init__()
        self._options = options
        self._script = ''
        self._query_list = []
        self._result_list = []
        self._configure()


    def _validate(self,options,template=CONFIG_TEMPLATE):
        #print options
        #print template
        for top_k in template.keys():
            #print top_k
            if top_k not in options.keys():
                if template[top_k] == True:
                    raise Exception('mandatory key %s missed'%top_k)
            else:
                #print template
                if type(template[top_k]) == types.DictType:
                    self._validate(options[top_k],template[top_k])
        return

    def _configure(self):
        self._defaultConfig()
        self.getDefaultConfig()
        #print self._options
        self._validate(self._options,CONFIG_TEMPLATE)
        #subclass validation
        self.validate()
        self._db_hosts = self._options['source']['hosts']

        if 'username' in self._options['source'].keys(): 
            self._db_username = self._options['source']['username']
        else:
            self._db_username = None

        if 'password' in self._options['source'].keys(): 
            self._db_password = self._options['source']['password']
        else:
            self._db_password = None

        self._db_name = self._options['source']['dbname']
 
        self._querydsl = self._options['query']['querydsl']
        if 'params' in self._options['query']:
            self._query_param = self._options['query']['params']

        if 'scripts' in self._options:
            self._script = self._options['scripts']
 
        self._es_hosts = self._options['target']['hosts']
        self._es_index_str = self._options['target']['index']
        self._es_type_str = self._options['target']['type']
        if 'document_id' in self._options['target'].keys(): 
            self._es_document_id_str = self._options['target']['document_id']
        else:
            self._es_document_id_str = None

        #subclass configure
        self.configure()
        
    def _defaultConfig(self):
        if 'target' not in self._options:
            self._options['target'] = {}
        if 'hosts' not in self._options['target']:
            self._options['target']['hosts'] = config.ES_HOSTS

    @abstractmethod
    def getDefaultConfig(self):
        """
        getDefaultConfig
        """
          
    #@abstractmethod
    #def validate(self):
    #    pass 

    @abstractmethod
    def configure(self):
        """
        configure
        """

    @abstractmethod
    def prepareCursor(self):
        """
        prepareCursor
        """

    def query(self):
        self.prepareCursor()
        self.formatQueryDsl()
        self.excuteQuery()

    @abstractmethod
    def formatQueryDsl(self):
        """
        formatQueryDsl
        """


    @abstractmethod
    def excuteQuery(self):
        """
        excuteQuery
        """
    
    def processData(self):
        if self._script:
            content = ''
            try:
                filepath = os.path.join(config.JOB_SCRIPT_PATH,self._script)
                f = open(filepath,'r')
                content = ('').join(f.readlines())
                f.close()
            except:
                raise Exception('open script file error')

            exec(textwrap.dedent(content))
        else:
            self._result_list = self._query_list

        print self._result_list

    def getRealStr(self,str,ele):
        m = re.findall('%\{(\S+)\}',str)
        if len(m) == 0:
            return str
        
        replace_map = {}
        for p in m:
            if p in replace_map.keys():
                continue
            sub = re.search('^\[([\d|\w|_|-]+)\]$',p)
            if sub:
                if sub.group(1) not in ele.keys():
                    raise Exception('es config error')    
                replace_map[p] = ele[sub.group(1)] 
                continue
            
            now = datetime.datetime.now()
            replace_map[p] = now.strftime(p)

        real_str = str
        print replace_map
        for p in replace_map.keys():
            real_str = re.subn('%\{'+ re.escape(p)+'\}',repr(replace_map[p]),real_str)[0]
        return real_str
             

    def actionsGen(self):
        index = 0
        for ele in self._result_list:
            param = {}
            param['_index'] = getRealStr(self._es_index_str,ele)
            param['_type'] = getRealStr(self._es_type_str,ele)
            if self._es_document_id:
                param['_id'] = getRealStr(self._es_documemt_id,ele)
            param['_source'] = ele
            yield param
            index = index + 1

    def saveData(self):
        #try:
        es = Elasticsearch(hosts=self._es_hosts)
        #except:
        #    raise Exception('initial es fail')
        if len(self._result_list) == 0:
            raise Exception("query result set is empty")
        
        (success_num,err_num) = helpers.bulk(es,self.actionsGen(),True)
        if err_num > 0:
            raise Exception("Elasticsearch bulk with error")
        return "Elasticsearch bulk success"

    def getLastSuccessTime(self):
        conn = MongoClient("localhost",27017)
        self.db = conn['orderMonitor']
        lastSuccessTime = None
        try:
            print self._job_id
            doc = self.db.jobResultStore.find({'jobId':self._job_id,'status':'success'}).sort('lastRunTime',DESCENDING).limit(1)
            lastSuccessTime = dt2str(utc2local(doc[0]['lastRunTime']),'%Y-%m-%d %H:%M:%S')
        except Exception,e:
            raise e
        finally:
            conn.close()

        return lastSuccessTime 

    def jobEntry(self,**kwargs):
        if 'job_id' in kwargs.keys():
            self._job_id = kwargs['job_id']
        #self._configure()
        self.query()
        self.processData()
        #return self.saveData()


class MysqlDataJob(DataJobTemplate):

    def __init__(self,options):
        super(MysqlDataJob,self).__init__(options)
        
    def getDefaultConfig(self):
        if 'source' not in self._options:
            self._options['source'] = {}
        if 'hosts' not in self._options['source'].keys():
            self._options['source']['hosts'] = config.MYSQL_HOSTS
        if 'dbname' not in self._options['source'].keys():
            self._options['source']['dbname'] = config.MYSQL_DBNAME
        if 'username' not in self._options['source'].keys():
            self._options['source']['username'] = config.MYSQL_USERNAME
        if 'password' not in self._options['source'].keys():
            self._options['source']['password'] = config.MYSQL_PASSWORD

    def validate(self):
        if type(self._options['query']['params']) != types.DictType:
            raise Exception('params should be list')
        import re
        self._query_dsl_slot = re.findall(':([\d|\w|_|-]+)',self._options['query']['querydsl'],re.I)
        #if len(slot) != len(self._options['query']['params']):
        #    raise Exception('params number mismatch')

        for param in self._query_dsl_slot:
            if param not in self._options['query']['params']:
                raise Exception('params name mismatch')

    def configure(self):
        self._options = self._options

    def prepareCursor(self):
        conn = MySQLdb.connect(host=self._db_hosts[0],
                                    port=self._db_hosts[1],
                                    user=self._db_username,
                                    passwd=self._db_password,
                                    db=self._db_name,
                                    charset='utf8')
        self._cursor = conn.cursor(cursorclass = MySQLdb.cursors.DictCursor)

    def formatQueryDsl(self):
        self._query_str = self._querydsl
        if self._query_param:
            for param in self._query_param.keys():
                value = self._query_param[param]
                if value == 'lastSuccessTime':
                    value = self.getLastSuccessTime()
                self._query_str = re.subn(':'+param,'\''+ value +'\'',self._query_str,re.I)[0]        

        print self._query_str

    def excuteQuery(self):
        try:
        #if True:
            result = self._cursor.execute(self._query_str)
            self._query_list = list(self._cursor.fetchall())
            print self._query_list
        except Exception,e:
            raise e


if __name__ == '__main__':
    options = {
               'query':{
                    'querydsl':'select * from custom_orders where create_time > :start and kjt_code = :kjt_code',
                    'params':{'start':'lastSuccessTime','kjt_code':'bhkjt'}
                   },
               'scripts':'orderinfo.py',
               'target': {
                    'index':'orderinfo-%{%Y-%m}',
                    'type':'orderinfo',
                    'document_id':'%{[merchant_order_id]}'
                 }
              }
    mysqlJob = MysqlDataJob(options)
    mysqlJob.jobEntry()
