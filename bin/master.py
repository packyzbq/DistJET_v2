import os,sys
import subprocess
import traceback
if 'DistJETPATH' not in os.environ:
    os.environ['DistJETPATH'] = "/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2"

sys.path.append(os.getenv('DistJETPATH'))
# check boost.python if exist or if JUNO offline software has been sourced
if not os.getenv('JUNOTOP'):
#    print 'Please setup JUNO official software first!'
#    exit()
    print 'No Env of Juno offline software detected'
    
if 'Boost' not in os.environ['PATH']:
    print("can't find Boost.Python, please setup Boost.Python first")
    exit()
    #rc = subprocess.Popen(['source', '/afs/ihep.ac.cn/users/z/zhaobq/env'])
	# this the python copy of the Boost/bashrc
else:
    print('SETUP: find Boost')


# argv[1] = appfile, argv[2]=config, argv[3]=log_level, argv[4]=app_config_file
if len(sys.argv) < 3:
    print('@master need at least 2 parameter(given %d), args=%s, exit'%(len(sys.argv)-1, sys.argv))
    exit()

if os.path.exists(sys.argv[1]):
    module_path = os.path.dirname(sys.argv[1])
    module_path = os.path.abspath(module_path)
    sys.path.append(module_path)
    module_name = os.path.basename(sys.argv[1])
    if module_name.endswith('.py'):
        module_name = module_name[:-3]

else:
    print('@master: cannot find app module %s, exit'%sys.argv[1])

rundir = os.getcwd()
from python.Util.Conf import Config
Config.setCfg('Rundir',rundir)

try:
    module = __import__(module_name)
except ImportError:
    print('@master: import user define module error, exit=%s'%traceback.format_exc())
    exit()

import python.Util.logger as logger
logger.setlevel(sys.argv[3])

from python.JobMaster import JobMaster
applications = []

if module.__dict__.has_key('run') and callable(module.__dict__['run']):
    #if sys.argv[4] == 'null':
    #    applications.append(module.run())
    #else:
    app = module.run(sys.argv[4])
    print app
    applications.extend(app)
else:
    print('@master: No callable function "run" in app module, exit')
    exit()
#load config file
from python.Util import Conf
cfg_path = sys.argv[2]
if cfg_path == 'null' or os.path.exists(os.path.abspath(cfg_path)):
        cfg_path = os.getenv('DistJETPATH') + '/config/default.cfg'
Conf.set_inipath(cfg_path)
cfg = Conf.Config()

master = JobMaster(applications=applications)
#else:
#    master = JobMaster(sys.argv[2],applications)
if master.getRunFlag():
    print('@master start running')
    master.startProcessing()

