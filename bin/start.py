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

parser = OptionParser(usage="%prog AppFile [opts]",description="start the master on local/HTCondor with config file")
parser.add_option("--batch", dest="batch", choices = ["condor","lsf","local"], default = "local", help="Batch job backend")
parser.add_option("-d", "--debug", dest="debug", action="store_true")
parser.add_option("-i", "--ini", dest="ini_file", help=" The initial configure file")
parser.add_option("-n", "--num", dest="worker_num", help="The executable worker number")
parser.add_option("-c", "--capacity",dest="capacity", help="The capacity of each worker")
#parser.add_option("-cf", "--capacity-file", dest="capacity_file", help="The file defines the capacity of each worker")
parser.add_option("-m", "--master-host", dest="master_host", help="The host master runs on")
parser.add_option("-w", "--worker-host", dest="worker_host", help="The host worker runs on, can be a hostlist file")

(opts, args) = parser.parse_args()
parg_master = ''+args[0]
parg_worker = ''

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
        capactiy = 1
        parg_worker+= ' 1'
    elif not str(opts.capacity).isdigit():
        print('Error: Capacity is not a valid number, exit')
        exit()
    else:
        capactiy = int(opts.capacity)
        parg_worker+='%d'%capactiy

    # config file
    if not opts.ini_file or not os.path.exists(opts.ini_file):
        print 'Can not find initial configure file input, use default configuration'
        config_file = None
        parg_master += ' null'
        parg_worker += ' null'
    else:
        config_file = opts.ini_file
        parg_master += ' ' + opts.conf_file
        parg_worker += ' ' + opts.conf_file

    # log level
    if opts.debug:
        parg_master += ' debug'
    else:
        parg_master += ' info'

    print "mpiexec python %s/bin/master.py %s"%(os.environ['DistJETPATH'],parg_master)
    master_rc = subprocess.Popen(['mpiexec','python',os.environ['DistJETPATH']+'/bin/master.py',parg_master], stdout=subprocess.PIPE,stderr=subprocess.STDOUT, shell = True)
    while True:
        fs = select.select([master_rc.stdout],[],[])
        if not fs[0]:
            pass
        if master_rc.stdout in fs[0]:
            record = os.read(master_rc.stdout.fileno(),1024)
            if record and record == '@master start running':
                break
            if record and 'exit' in record:
                print('Error occurs when start master, msg = %s'%record)
                exit()
    print "mpiexec -n %s python %s/bin/worker.py %s"%(worker_num,os.environ['DistJETPATH'],parg_worker)
    worker_rc = subprocess.Popen(['mpiexec','-n',worker_num, 'python', os.environ['DistJETPATH']+'/bin/worker.py', parg_worker], stdout=subprocess.PIPE,stderr=subprocess.STDOUT, shell=True)

    master_log = open('master.log','w+')
    worker_log = open('worker.log','w+')
    while True:
        fs = select.select([master_rc.stdout, worker_rc.stdout],[],[])
        if not fs[0]:
            break
        if master_rc.stdout in fs[0]:
            record =os.read(master_rc.stdout.fileno(),1024)
            if record:
                master_log.write(record)
        if worker_rc.stdout in fs[0]:
            record = os.read(worker_rc.stdout.fileno(),1024)
            if record:
                worker_log.write(record)
    worker_rc.wait()
    master_rc.wait()







