import os,sys
import re
import traceback

# argv[1] = appfile, argv[2]=config, argv[3]=log_level
if len(sys.argv) < 3:
    print('@master need at least 2 parameter(given %d), exit'%(len(sys.argv)-1))
    exit()

svc_name = None

if os.path.exists(sys.argv[1]):
    module_path = os.path.dirname(sys.argv[1])
    sys.path.append(module_path)
    module_name = os.path.basename(sys.argv[1])
    if module_name.endswith('.py'):
        module_name = module_name[:-3]

else:
    print('@master: cannot find app module %s, exit'%sys.argv[1])

try:
    module = __import__(module_name)
except ImportError:
    print('@master: import user define module error, exit=%s'%traceback.format_exc())
    exit()

from python.JobMaster import JobMaster
applications = []

if module.__dict__.has_key('run') and callable(module.__dict__['run']):
    applications.append(module.run())
else:
    print('@master: No callable function "run" in app module, exit')
    exit()

if sys.argv[2] == 'null':
    master = JobMaster(applications=applications)
else:
    master = JobMaster(sys.argv[2],applications)
print('@master start running')
master.startProcessing()

