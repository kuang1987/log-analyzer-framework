#!/usr/bin/env python
import sys
from traceback import format_tb
try:
    raise IOError("123")
except:
    print sys.exc_info()
    print sys.exc_info()[1]
    print ''.join(format_tb(sys.exc_info()[-1]))
