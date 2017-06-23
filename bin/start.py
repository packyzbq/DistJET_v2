#!/usr/bin/python
from optparse import OptionParser
from multiprocessing import Process,Pool
from multiprocessing import Queue as multiQueue
import subprocess
import select
import os,sys
import traceback

if 'DistJETPATH' not in os.environ:
    os.environ['DistJETPATH'] = "/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2"
sys.path.append(os.environ['DistJETPATH'])

parser = OptionParser(usage="%prog AppFile WorkerFile [opts]",description="start the master on local/HTCondor with config file")
parser.add_option("--batch", dest="batch", choices = ["condor","lsf","local"], default = "local", help="Batch job backend")
parser.add_option("-d", "--debug", dest="debug", action="store_true")
parser.add_option("-i", "--ini", dest="ini_file", help=" The initial configure file")
parser.add_option("-n", "--num", dest="worker_num", help="The executable worker number")
parser.add_option("-c", "--capacity",dest="capacity", help="The capacity of each worker")
#parser.add_option("-cf", "--capacity-file", dest="capacity_file", help="The file defines the capacity of each worker")
parser.add_option("-m", "--master-host", dest="master_host", help="The host master runs on")
parser.add_option("-w", "--worker-host", dest="worker_host", help="The host worker runs on, can be a hostlist file")

(opts, args) = parser.parse_args()

if opts.batch == "local":
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

    # check parameter
    # worker number
    if not opts.worker_num:
        print('Warning: No worker number input, will start with ONE worker')
        worker_num = 1
    elif not str(opts.worker_num).isdigit():
        print('Error: Worker number is not a valid number, exit')
        exit()
    else:
        worker_num=int(opts.worker_num)
    # capacity
    if not opts.capacity:
        print("Warning: No capacity input, start with One capactiy")
        capacity = '1'
    elif not str(opts.capacity).isdigit():
        print('Error: Capacity is not a valid number, exit')
        exit()
    else:
        capacity = opts.capacity

    # config file
    if not opts.ini_file or not os.path.exists(opts.ini_file):
        print 'Can not find initial configure file input, use default configuration'
        config_file = 'null'
    else:
        config_file = opts.ini_file
    # log level
    if opts.debug:
        level='debug'
    else:
        level = 'info'

    print "mpiexec python %s/bin/master.py %s,%s,%s"%(os.environ['DistJETPATH'],args[0],config_file,level)
    master_rc = subprocess.Popen(['mpiexec','python',os.environ['DistJETPATH']+'/bin/master.py',args[0],config_file,level], stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    while True:
        fs = select.select([master_rc.stdout],[],[])
        if not fs[0]:
            print "No output of master, exit()"
            exit()
        if master_rc.stdout in fs[0]:
            record = os.read(master_rc.stdout.fileno(),1024)
            print record
            if record and '@master start running' in record:
                break
            if record and 'exit' in record:
                print('Error occurs when start master, msg = %s'%record)
                exit()
    print "mpiexec -n %s python %s/bin/worker.py %s,%s,%s"%(worker_num,os.environ['DistJETPATH'],capacity,config_file, args[1])
    worker_rc = subprocess.Popen(['mpiexec','-n',str(worker_num), 'python', os.environ['DistJETPATH']+'/bin/worker.py',str(capacity),config_file, args[1]], stdout=subprocess.PIPE,stderr=subprocess.STDOUT)

    master_log = open('master.log','w+')
    worker_log = open('worker.log','w+')
    while (master_rc.poll() is None) or (worker_rc.poll() is None):
        fs = select.select([master_rc.stdout, worker_rc.stdout],[],[])
        if not fs[0]:
            break
        if master_rc.stdout in fs[0]:
            record =os.read(master_rc.stdout.fileno(),1024)
            if record:
                master_log.write(record)
                print "Master: %s"%record
        if worker_rc.stdout in fs[0]:
			# FIXME worker_rc block by child process, cause record block
            record = os.read(worker_rc.stdout.fileno(),1024)
            if record:
                worker_log.write(record)
                print "Worker: %s"%record
    #FIXME: the script worker runs may process childprocess and cause this method block
    worker_rc.wait()
    master_rc.wait()







