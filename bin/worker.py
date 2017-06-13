import sys,os
if 'DistJETPATH' not in os.environ:
    os.environ['DistJETPATH'] = "/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2"
sys.path.append(os.getenv('DistJETPATH'))
import subprocess
#argv[1]=capacity, argv[2]=conf_file
if len(sys.argv) <= 2:
    print('@worker, need at least 2 parameter(given %d), exit'%(len(sys.argv)-1))
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
agent = {}

# TODO: add multiprocess pool
# pool = multiprocessing.Pool(processes=worker_num)
agent[0] = WorkerAgent.WorkerAgent(sys.argv[2],capacity)
agent[0].run()
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
