from sys import argv
import multiprocessing

#argv[1] = num, argv[2]=capacity, argv[3]=conf_file
if len(argv) <= 3:
    print('@worker, need at least 3 parameter(given %d), exit'%(len(argv)-1))
    exit()

from python import WorkerAgent
worker_num = argv[1]
capacity = argv[2]
agent = {}

# TODO: add multiprocess pool
# pool = multiprocessing.Pool(processes=worker_num)

for i in range(0,worker_num):
    agent[i] = WorkerAgent.WorkerAgent(argv[3],capacity)
    agent[i].start()
for a in agent.values():
    a.join()
