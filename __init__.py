import sys,os

base = os.path.dirname(os.path.abspath(__file__))


sys.path.append(os.path.join(base,'jobs'))
sys.path.append(os.path.join(base,'controller'))
print sys.path
