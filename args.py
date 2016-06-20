#!/usr/bin/env python

def func(*args,**kwargs):
    print "args=="
    for arg in args:
        print arg
    print "kwargs"
    for k in kwargs:
        print "%s=%s"%(str(k),str(kwargs[k]))


func([3,4],a=1,b=2,c={'a':'1'})
