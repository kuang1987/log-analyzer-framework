#!/usr/bin/env python

import tornado.web
from tornado.escape import json_decode
import time
import json
import os,sys
from utils.validator import validator
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'../'))

CREATE_JOB_SCHEMA = {"type":"object",
                     "properties":{
                                   "jobName":{"type":"string"},
                                   "jobType":{"type":"string",
                                              "enum":["gw","ecc","es"]},
                                   "jobScheType":{"type":"string",
                                                  "enum":["in","cron"]},
                                   "jobScheParams":{"type":"string"},
                                   "querydsl":{"type":"string"},
                                   "queryparams":{"type":"object"},
                                   "scripts":{"type":"string"},
                                   "es_index":{"type":"string"},
                                   "es_type":{"type":"string"},
                                   "es_document_id":{"type":"string"}
                                  },
                     "required":["jobName","jobType","jobScheType","jobScheParams","querydsl","es_index","es_type"]}

class JobsHandler(tornado.web.RequestHandler):

    def prepare(self):
        try:
            self.body_json = json_decode(self.request.body)
            self.add_header("Access-Control-Allow-Origin","http://localhost:8080")
        except:
            self.write(json.dumps({"code":-1,"reason":"params parse failure"}))
            self.finish()
            return

    def get(self,version,jobId=None):
        print version
        print jobId
        if(jobId):
            return self.write(jobId)
        else:
            return self.write("job list")

    @validator(CREATE_JOB_SCHEMA)
    def post(self,version,jobId):
        print self.body_json
        return self.write("create")

    def put(self,version,jobId):
        return self.write("method: " + self.request.method + "  " + jobId)

    def delete(self,version,jobId):
        return self.write("method: " + self.request.method + "  " + jobId)
  
   
