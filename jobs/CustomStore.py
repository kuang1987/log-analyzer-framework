#!/usr/bin/env python

from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.base import ConflictingIdError

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    from bson.binary import Binary
    from pymongo.errors import DuplicateKeyError
except ImportError:  # pragma: nocover
    raise ImportError('MongoDBJobStore requires PyMongo installed')
from apscheduler.util import datetime_to_utc_timestamp


class CustomStore(MongoDBJobStore):

    def __init__(self,*args,**kwargs):
        super(CustomStore,self).__init__(*args,**kwargs)

    def add_job(self, job):
        state = job.__getstate__()
        state['kwargs']['job_id'] = job.id
        job.__setstate__(state)
        try:
            self.collection.insert({
                '_id': job.id,
                'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
                'job_state': Binary(pickle.dumps(job.__getstate__(), self.pickle_protocol))
            })
        except DuplicateKeyError:
            raise ConflictingIdError(job.id)
