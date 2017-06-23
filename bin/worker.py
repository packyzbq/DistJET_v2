import sys,os
if 'DistJETPATH' not in os.environ:
    os.environ['DistJETPATH'] = "/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2"
sys.path.append(os.getenv('DistJETPATH'))
import subprocess
#argv[1]=capacity, argv[2]=conf_file argv[3]=appdir/worker_class
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

import python.Util.Conf as Conf
Conf.Config.setCfg('Rundir',os.getcwd())
if sys.argv[2] != 'null' and os.path.exists(sys.argv[2]):
    Conf.set_inipath(os.path.abspath(sys.argv[2]))

from python import WorkerAgent
capacity = int(sys.argv[1])
worker_module_path = None
if os.path.exists(sys.argv[3]):
    worker_module_path = os.path.abspath(sys.argv[3])
    print 'find specific worker module %s'%os.path.basename(worker_module_path)
elif os.path.exists(os.environ['DistJETPATH']+'/'+sys.argv[3]):
    worker_module_path = os.path.abspath(os.environ['DistJETPATH']+'/python/Application/'+sys.argv[3])
    print 'find specific worker module %s' % os.path.basename(worker_module_path)

# TODO: add multiprocess pool
# pool = multiprocessing.Pool(processes=worker_num)
agent = WorkerAgent.WorkerAgent(sys.argv[2],capacity,worker_module_path)
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
