#!/usr/bin/env python
#coding=utf-8
#builtin
from datetime import datetime
import os,sys
import pytz

#tornado related
import tornado.auth
import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options

#pymong
from pymongo import MongoClient

#apscheduler related
from apscheduler.schedulers.tornado import TornadoScheduler
from apscheduler.executors.pool import ThreadPoolExecutor,ProcessPoolExecutor
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.events import EVENT_JOB_EXECUTED,EVENT_JOB_ERROR,EVENT_JOB_MISSED
#controller
from urls import handlers

from jobs.CustomStore import CustomStore




define("port", default=8080, help="run on the given port", type=int)

class Application(tornado.web.Application):
    def __init__(self):
        jobstores = {
            'mongo': CustomStore(host='localhost', port=27017),
        }
        executors = {
            'default': ThreadPoolExecutor(20),
            'processpool': ProcessPoolExecutor(5)
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 1
        }
        self.scheduler = TornadoScheduler(jobstores=jobstores,executors=executors,job_defaults=job_defaults,timezone=pytz.timezone('Asia/Shanghai'))
        self.scheduler.add_listener(self.schedulerListener,EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED)

      
        settings = dict(
            template_path = os.path.join(os.path.dirname(__file__),"view"),
            static_path = os.path.join(os.path.dirname(__file__),"static"),
            debug=False
        )
    
        conn = MongoClient("localhost",27017)
        self.db = conn['orderMonitor']
        self.prepareJobResultStore()
        tornado.web.Application.__init__(self,handlers,**settings)

    def prepareJobResultStore(self):
        self.db.jobResultStore.create_index("jobId")
        self.db.jobResultStore.create_index("status")
        self.db.jobResultStore.create_index("lastRunTime")

    def schedulerListener(self,ev):
        jobResult = {"jobId":ev.job_id,
                     "lastRunTime":ev.scheduled_run_time,
                     "status":"",
                     "desc":""}
        if ev.code & EVENT_JOB_EXECUTED:
            jobResult['status'] = "success"
            jobResult['desc'] = str(ev.retval)
        elif ev.code & EVENT_JOB_ERROR:
            jobResult['status'] = "failure"
            jobResult['desc'] = repr(ev.exception) + '\n' + str(ev.traceback)
            
        self.db.jobResultStore.insert(jobResult) 

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    http_server.request_callback.scheduler.start()
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
