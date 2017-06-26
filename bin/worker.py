import sys,os
if 'DistJETPATH' not in os.environ:
    os.environ['DistJETPATH'] = "/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2"
sys.path.append(os.getenv('DistJETPATH'))
import subprocess
#argv[1]=capacity, argv[2]=conf_file
if len(sys.argv) <= 3:
    print('@worker, need at least 3 parameter(given %d), exit'%(len(sys.argv)-1))
    exit()

if 'Boost' not in os.environ['PATH']:
    print("can't find Boost.Python, please setup Boost.Python first")
    exit()
    #print("can't find Boost.Python, setup Boost")
    #subprocess.Popen(['source', '/afs/ihep.ac.cn/users/z/zhaobq/env'])
else:
    print('SETUP: find Boost')
from python.Util import Conf
Conf.Config.setCfg('Rundir',os.getcwd())
if sys.argv[2] != 'null' and os.path.exists(sys.argv[2]):
    Conf.set_inipath(os.path.abspath(sys.argv[2]))
#load config file
cfg_path = sys.argv[2]
if cfg_path == 'null' or os.path.exists(os.path.abspath(cfg_path)):
    print 'No config file input, use default config'
    cfg_path = os.getenv('DistJETPATH') + '/config/default.cfg'
Conf.set_inipath(cfg_path)
cfg = Conf.Config()

from python import WorkerAgent
capacity = int(sys.argv[1])
worker_module_path = None
'''
worker_file = sys.argv[3]
if not worker_file.endswith('.py'):
    worker_file = worker_file+'.py'
if os.path.exists(worker_file):
    worker_module_path = os.path.abspath(worker_file)
    print 'find specific worker module %s'%os.path.basename(worker_module_path)
elif os.path.exists(os.environ['DistJETPATH']+'/python/Application/'+worker_file):
    worker_module_path = os.path.abspath(os.environ['DistJETPATH']+'/python/Application/'+worker_file)
    print 'find specific worker module %s' % os.path.basename(worker_module_path)
else:
    fflag = False
    for dd in os.listdir(os.environ['DistJETPATH']+'/Application'):
        prepath = os.environ['DistJETPATH']+'/Application/'+dd
        if os.path.exists(prepath+'/'+worker_file):
            fflag = True
            worker_module_path = prepath+'/'+worker_file
    if not fflag:
        print 'Can not find specific worker module ,use default'
'''
agent = WorkerAgent.WorkerAgent(capacity)
agent.run()
import threading
print('Worker Agent exit, remains %d thread running, threads list = %s'%(threading.active_count(),threading.enumerate()))
exit()
'''
for i in range(0,worker_num):
    agent[i] = WorkerAgent.WorkerAgent(sys.argv[3],capacity)
    agent[i].start()
for a in agent.values():
    a.join()
'''
