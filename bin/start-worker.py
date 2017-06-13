from optparse import OptionParser
import os, sys
import subprocess

if 'DistJETPATH' not in os.environ:
    os.environ['DistJETPATH'] = "/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2"

parser = OptionParser(usage="%prog [Options]")
parser.add_option("-n","--num", dest="worker_num")
parser.add_option("-c","--cap",dest="capacity")
# FIXME: no need to give the config ?
# parser.add_option('-f','--file', dest='capacity_file',help='tell the host and capacity for each worker')
parser.add_option('-b','--back',dest='back',action='store_true',help='run worker in background')
parser.add_option('-g',"--debug", dest="debug",action="store_true")
parser.add_option('--conf', dest='conf_file')

(opts, args) = parser.parse_args()

parg = ''

# check env
try:
    rc = subprocess.Popen(["mpich2version"], stdout=subprocess.PIPE)

    print('SETUP: find mpich tool')
except:
    print("can't find mpich tool, please setup mpich2 first")
    exit()


# check mpd running
rc = subprocess.Popen(['mpdtrace'], stdout=subprocess.PIPE)
stdout = rc.communicate()[0]
if 'no mpd is running' in stdout:
    print('no mpd running, exit')
    exit()

if not opts.worker_num:
    print 'Lack worker numbers, exit'
    exit()
else:
    # one agent has one worker
    parg+='1'
    # TODO: testing performance
    # one agent has many workers
    #parg+=opts.worker_num


if not opts.capacity and not opts.capacity_file:
    print 'lack capacity information, exit'
    exit()
'''
if opts.capacity_file:
    if not os.path.exists(opts.capacity_file):
        print 'Cannot find the capacity file : %s, exit'%opts.capacity_file
        exit()
    else:
        cap_list = {} # hostname:capacity
        with open(opts.capacity_file,'r+') as f:
            for line in f.readlines():
                cap_list[line.split(' ')[0]] = line.split(' ')[1]
'''
if opts.capacity:
    parg=""+opts.capacity
if not opts.conf_file or not os.path.exists(opts.conf_file):
    print 'No config file input, use default configuration'
    parg+=' null'
else:
    parg+= ' '+opts.conf_file

# this is the plan A-> each workerAgent has one worker
print('mpiexec -n %s python $DistJETPATH/bin/worker.py %s'%(opts.worker_num,parg))
os.system("mpiexec -n %s python $DistJETPATH/bin/worker.py %s"%(opts.worker_num,parg))
print("worker stop...")




