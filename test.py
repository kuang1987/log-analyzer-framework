#!/usr/bin/env python

import textwrap
import sys
from backports_abc import Generator as GeneratorType


def a(*args,**kargs):
    kargs['id'] = 3
    print kargs

a(a=1,b=2)
