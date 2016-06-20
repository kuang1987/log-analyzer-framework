from controller import *

handlers = [
    ('/',indexController.indexHandler),
    ('/api/(v[0-9]?)/jobs/(.*)',dataJobController.JobsHandler),
    ('/api/.*',indexController.indexHandler),
    ('/.*',indexController.indexHandler)
]
