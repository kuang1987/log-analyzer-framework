#!/usr/bin/env python
import json
from jsonschema import validate
from jsonschema.exceptions import  ValidationError

def validator(schema):
    def wrapper(f):
        def subwrapper(*args,**kwargs):
            self = args[0]
            try:
                validate(self.body_json,schema)
            except ValidationError,e:
                self.write(json.dumps({"code":-1,"reason":str(e.message)}))
                self.finish()
                return
    
            return f(*args,**kwargs)
        return subwrapper

    return wrapper

