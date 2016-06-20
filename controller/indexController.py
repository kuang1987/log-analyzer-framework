#!/usr/bin/env python
import tornado.web
import re

class indexHandler(tornado.web.RequestHandler):
    def prepare(self):
        path = self.request.path
        if re.search('/api/',path,re.I):
            self.set_status(404,"Not Found")
            self.finish()
            return

    def get(self):
        return self.write("/index.html")
  
