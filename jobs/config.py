#!/usr/bin/env python

import os,sys

basedir = os.path.dirname(os.path.abspath(__file__))

JOB_SCRIPT_PATH = os.path.join(basedir,'scripts/')

#mysql default

MYSQL_HOSTS = ['10.10.2.15',3306]
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = 'kjt123456'
MYSQL_DBNAME = 'wmshub_service'

#es default
ES_HOSTS = ['10.10.6.145:9200','10.10.5.146:9200']
