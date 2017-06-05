import sys,os
sys.path.append(os.getenv('DistJETPATH'))
import subprocess
#argv[1] = num, argv[2]=capacity, argv[3]=conf_file
if len(sys.argv) <= 3:
    print('@worker, need at least 3 parameter(given %d), exit'%(len(argv)-1))
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
if sys.argv[3] != 'null' and os.path.exists(sys.argv[3]):
    Conf.set_inipath(os.path.abspath(sys.argv[3]))

from python import WorkerAgent
worker_num = int(sys.argv[1])
capacity = int(sys.argv[2])
agent = {}

# TODO: add multiprocess pool
# pool = multiprocessing.Pool(processes=worker_num)
agent[0] = WorkerAgent.WorkerAgent(sys.argv[3],capacity)
agent[0].run()
'''
for i in range(0,worker_num):
    agent[i] = WorkerAgent.WorkerAgent(sys.argv[3],capacity)
    agent[i].start()
for a in agent.values():
    a.join()
'''
