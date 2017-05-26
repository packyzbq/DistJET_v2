import Queue
import datetime
import json
import os
import select
import subprocess
import threading
import multiprocessing
import time
import traceback

import IRecv_Module as IM

import HealthDetect as HD
from BaseThread import BaseThread
from MPI_Wrapper import Tags ,Client
from Util import logger
from WorkerRegistry import WorkerStatus
from python.Util import Conf

log = logger.getLogger('WorkerAgent')
wlog = None

def MSG_wrapper(**kwd):
    return json.dumps(kwd)

class status:
    (SUCCESS, FAIL, TIMEOUT, OVERFLOW, ANR) = range(0,5)
    DES = {
        FAIL: 'Task fail, return code is not zero',
        TIMEOUT: 'Run time exceeded',
        OVERFLOW: 'Memory overflow',
        ANR: 'No responding'
    }
    @staticmethod
    def describe(stat):
        if status.DES.has_key(stat):
            return status.DES[stat]

class HeartbeatThread(BaseThread):
    """
    ping to master, provide information and requirement
    """
    def __init__(self, client, worker_agent):
        BaseThread.__init__(self, name='HeartbeatThread')
        self._client = client
        self.worker_agent = worker_agent
        self.queue_lock = threading.RLock()
        self.acquire_queue = Queue.Queue()         # entry = key:val
        self.interval = 1

    def run(self):
        #add first time to ping master
        send_dict = {}
        send_dict['flag'] = 'FirstPing'
        send_dict[Tags.MPI_REGISTY] = {'uuid':self.worker_agent.uuid,'capacity':self.worker_agent.capacity}
        send_dict['ctime'] = datetime.datetime.now()
        send_dict['uuid'] = self.worker_agent.uuid
        send_str = json.dumps(send_dict)
        self._client.send_string(send_str, len(send_str),0,Tags.MPI_REGISTY)

        while not self.get_stop_flag():
            try:
                self.queue_lock.acquire()
                send_dict.clear()
                send_dict = dict(send_dict, **self.worker_agent.health_info)
                while not self.acquire_queue.empty():
                    tmp_d = self.acquire_queue.get()
                    if send_dict.has_key(tmp_d.keys()[0]):
                        wlog.warning('[HeartBeatThread]: Reduplicated key=%s when build up heart beat message, skip it')
                        continue
                    send_dict = dict(send_dict, **tmp_d)
                self.queue_lock.release()
                send_dict['Task'] = []
                while not self.worker_agent.task_completed_queue.empty():
                    task = self.worker_agent.task_completed_queue.get()
                    send_dict['Task'].append(task)
                send_dict['uuid'] = self.worker_agent.uuid
                send_dict['wid'] = self.worker_agent.wid
                send_dict['health'] = self.worker_agent.health_info()
                send_dict['rTask'] = self.worker_agent.worker.running_task
                send_dict['ctime'] = datetime.datetime.now()
                send_str = json.dumps(send_dict)
                self._client.send_string(send_str, len(send_str), 0, Tags.MPI_PING)
            except Exception:
                wlog.error('[HeartBeatThread]: unkown error, thread stop. msg=%s', traceback.format_exc())
            else:
                time.sleep(self.interval)

        # the last time to ping Master
        if not self.acquire_queue.empty():
            remain_command = ''
            while not self.acquire_queue.empty():
                remain_command+=self.acquire_queue.get().keys()
            log.waring('[HeartBeat] Acquire Queue has more command, %s, ignore them'%remain_command)
        send_dict.clear()
        send_dict['wid'] = self.worker_agent.wid
        send_dict['uuid'] = self.worker_agent.uuid
        send_dict['flag'] = 'lastPing'
        send_dict['Task'] = []
        while not self.worker_agent.task_completed_queue.empty():
            task = self.worker_agent.task_completed_queue.get()
            send_dict['Task'].append(task)
        # add node health information
        send_dict['health'] = self.worker_agent.health_info()
        send_dict['ctime'] = datetime.datetime.now()
        send_str = json.dumps(send_dict)
        self._client.send_string(send_str, len(send_str), 0, Tags.MPI_DISCONNECT)



    def set_ping_duration(self, interval):
        self.interval = interval

class WorkerAgent(multiprocessing.Process):
    """
    agent
    """
    def __init__(self,cfg_path=None, capacity=1):
        multiprocessing.Process.__init__(self)
        if not cfg_path:
            #use default path
            cfg_path = os.getenv('DistJETPATH')+'/config/default.cfg'
        Conf.set_inipath(cfg_path)

        self.recv_buff = IM.IRecv_buffer()
        self.__should_stop_flag = False
        import uuid as uuid_mod
        self.uuid = str(uuid_mod.uuid4())
        self.client = Client(self.recv_buff, Conf.Config.getCFGattr('svc_name'), self.uuid)
        self.client.initial()
        self.cfg = Conf.Config()

        self.wid = None
        self.appid = None   #The running app id
        self.capacity = capacity
        self.task_queue = Queue.Queue(maxsize=self.capacity)
        self.task_completed_queue = Queue.Queue()

        self.tmpExecutor = {}
        self.tmpLock = threading.RLock()

        # The operation/requirements need to transfer to master through heart beat
        self.heartbeat = HeartbeatThread(self.client,self)

        self.ignoreTask = []
        self.cond = threading.Condition()
        self.worker = Worker(self,self.cond)
        self.worker.start()

    def start(self):
        self.heartbeat.run()
        while True:
            if not self.recv_buff.empty():
                recv_dict = json.loads(self.recv_buff.get())
                for k,v in recv_dict:
                    # registery info v={wid:val,init:{boot:v, args:v, data:v, resdir:v}, appid:v}
                    if int(k) == Tags.MPI_REGISTY_ACK:
                        try:
                            self.wid = v['wid']
                            self.appid = v['appid']
                            self.tmpLock.acquire()
                            self.tmpExecutor = v['init']
                            self.tmpLock.release()
                        except KeyError:
                            pass
                    # add tasks v={tid:{boot:v, args:v, data:v, resdir:v}, tid:....}
                    elif int(k) == Tags.TASK_ADD:
                        for tk,tv in v:
                            self.task_queue.put({tk,tv})
                        if self.worker.status == WorkerStatus.IDLE:
                            self.cond.acquire()
                            self.cond.notify()
                            self.cond.release()

                    # remove task, v=tid
                    elif int(k) == Tags.TASK_REMOVE:
                        if self.worker.running_task == v:
                            self.worker.kill()
                        else:
                            self.ignoreTask.append(v)
                    # master disconnect ack,
                    elif int(k) == Tags.LOGOUT:
                        if self.worker.status != WorkerStatus.FINALIZED:
                            log.error('logout error because of wrong worker status, worker status = %d', self.worker.status)
                            # TODO do something
                        self.cond.acquire()
                        self.cond.notify()
                        self.cond.release()
                        # stop worker agent
                        #self.heartbeat.acquire_queue.put({Tags.LOGOUT_ACK:self.wid})
                        break
                    # app finalize {fin:{boot:v, args:v, data:v, resdir:v}}
                    elif int(k) == Tags.APP_FIN:
                        self.tmpLock.acquire()
                        self.tmpExecutor = v['fin']
                        self.tmpLock.release()
                        self.worker.finialized = True
                        if self.worker.status == WorkerStatus.IDLE:
                            self.cond.acquire()
                            self.cond.notify()
                            self.cond.release()
                    # new app arrive, {init:{boot:v, args:v, data:v, resdir:v}, appid:v}
                    elif int(k) == Tags.NEW_APP:
                        # TODO new app arrive, need refresh
                        pass
            if self.task_queue.qsize() < self.capacity and self.worker.status == WorkerStatus.INITILAZED:
                self.heartbeat.acquire_queue.put({Tags.TASK_ADD:self.capacity-self.task_queue.qsize()})

        self.worker.join()
        log.info('[WorkerAgent] Worker thread has joined')
        self.stop()

    def stop(self):
        log.info('[WorkerAgent] Agent stop...')
        self.__should_stop_flag = True
        if self.heartbeat:
            self.heartbeat.stop()
            self.heartbeat.join()
        self.client.stop()

    def task_done(self, tid, task_stat,**kwd):
        tmp_dict = dict({'task_stat':task_stat},**kwd)
        self.task_completed_queue.put({tid:tmp_dict})

    def app_ini_done(self,returncode,errmsg=None):
        if returncode != 0:
            log.error('[Error] Worker initialization error, error msg = %s',errmsg)
        self.heartbeat.acquire_queue.put({Tags.APP_INI:{'wid':self.wid,'recode':returncode, 'errmsg':errmsg}})

    def app_fin_done(self, returncode, errmsg = None):
        if returncode != 0:
            log.error('[Error] Worker finalization error, error msg = %s',errmsg)
        self.heartbeat.acquire_queue.put({Tags.APP_FIN:{'wid':self.wid,'recode':returncode}})



    def _app_change(self, appid):
        """
        WorkerAgent calls when new app comes.
        :return:
        """
        #TODO
        pass

    def health_info(self):
        """
        Provide node health information which is transfered to Master
        Info: CPU-Usage, numProcessors, totalMemory, usedMemory
        Plug in self-costume bash scripts to add more information
        :return: dict
        """
        tmpdict = {}
        tmpdict['CpuUsage'] = HD.getCpuUsage()
        tmpdict['MemoUsage'] = HD.getMemoUsage()
        script = self.cfg.getCFGattr("health_detect_scripts")
        if script and os.path.exists(self.cfg.getCFGattr('topDir')+'/'+script):
            script = self.cfg.getCFGattr('topDir')+'/'+script
            rc = subprocess.Popen(executable=script,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            info, err = rc.communicate()
            if err=='':
                tmpdict['script_info'] = info
            else:
                tmpdict['script_err'] = err

        return tmpdict


# FIXME: worker initial in one process, cannot run task in other process. Should make init/run/finalize in the same process
# Load customed worker class  or  make the task script include init-do-fin steps
class Worker(BaseThread):
    def __init__(self, workagent, cond, worker_class=None):
        BaseThread.__init__(self,"worker")
        self.workeragent = workagent
        self.running_task = None
        self.cond = cond
        self.initialized = False
        self.finialized = False
        self.status = WorkerStatus.NEW
        self.log = wlog

        self.worker_obj = None
        if worker_class:
            self.worker_obj = worker_class(self.log)

        self.stdout = self.stderr = subprocess.PIPE
        self.process = None
        self.pid = None

        self.returncode = None
        self.fatalLine = None
        self.logParser = None
        self.task_status = None
        self.killed = None
        self.limit=None
        self.virLimit = None
        self.start_time = None

    def run(self):
        while not self.get_stop_flag():
            while not self.initialized:
                self.status = WorkerStatus.NEW
                self.cond.acquire()
                self.cond.wait()
                self.cond.release()

                self.workeragent.tmpLock.require()
                if cmp(['boot','args','data','resdir'], self.workeragent.tmpExecutor.keys()) == 0:
                    self.initialize(self.workeragent.tmpExecutor['boot'], self.workeragent.tmpExecutor['args'], self.workeragent.tmpExecutor['data'], self.workeragent.tmpExecutor['resdir'])
                self.workeragent.tmpLock.release()
                if not self.initialized:
                    continue
            wlog.inof('Worker Initialized, Ready to running tasks')
            self.status = WorkerStatus.RUNNING
            while not self.finialized:
                if not self.workeragent.task_queue.empty():
                    task = self.workeragent.task_queue.get()
                    if task in self.workeragent.ignoreTask:
                        continue
                    # TODO add task failed handle
                    tid,v = task.popitem()
                    self.do_task(v['boot'],v['args'],v['data'],v['resdir'])
                    self.workeragent.task_done(tid, self.task_status, time_start=self.start_time, time_fin=datetime.datetime.now(),errcode=self.returncode)
                else:
                    wlog.info('Worker: No task to do, Idle...')
                    self.status = WorkerStatus.IDLE
                    self.cond.acquire()
                    self.cond.wait()
                    self.cond.release()
            # do finalize
            # TODO according to Policy ,decide to force finalize or wait for all task done
            self.workeragent.tmpLock.acquire()
            self.finalize(self.workeragent.tmpExecutor['boot'], self.workeragent.tmpExecutor['args'],
                          self.workeragent.tmpExecutor['data'], self.workeragent.tmpExecutor['resdir'])
            self.workeragent.tmpLock.release()
            self.workeragent.app_fin_done(self.returncode, status.describe(self.returncode))


            # sleep or stop
            self.cond.acquire()
            self.cond.wait()
            self.cond.release()





    def initialize(self, boot, args, data, resdir, **kwargs):
        wlog.info('Worker start to initialize...')
        if self.worker_obj:
            if self.worker_obj.initialize(boot=boot, args=args,data=data,resdir=resdir):
                self.initialized = True
                self.status = WorkerStatus.INITILAZED
            else:
                wlog.error('Error: error occurs when initializing worker')
        elif not boot and not data:
            self.initialized = True
            self.status = WorkerStatus.INITILAZED
        else:
            if self.do_task(boot, args, data, resdir, flag='init'):
                self.initialized = True

    def do_task(self, boot, args, data, resdir,tid=0,**kwd):
        self.running_task = tid
        self.start_time = datetime.datetime.now()
        self.process = subprocess.Popen(args=[boot, args, data], stdout=self.stdout, stderr=self.stderr)
        self.pid = self.process.pid
        if len(resdir) and not os.path.exists(resdir):
            os.makedirs(resdir)
        if kwd.has_key('flag'):
            logFile = open('%s/%s_log'%(resdir,kwd['flag']),'w+')
        else:
            logFile = open('%s/app_%d_task_%d'%(resdir,self.workeragent.appid,tid), 'w+')
        if self.worker_obj:
            self.returncode = self.worker_obj.do_work(boot=boot, args=args, data=data, resdir=resdir, logfile=logFile)
        else:
            while True:
                fs = select.select([self.process.stdout], [], [], Conf.Config.getCFGattr('AppRespondTimeout'))
                if not fs[0]:
                    self.task_status = status.ANR
                    self.kill()
                    break
                if self.process.stdout in fs[0]:
                    record = os.read(self.process.stdout.fileno(),1024)
                    if not record:
                        break
                    logFile.write(record)
                    if self.logParser:
                        if not self._parseLog(record):
                            self.task_status = status.FAIL
                            self.kill()
                            break
                    if not self._checkLimit():
                        break
            self.returncode = self.process.wait()
        if self.status:
            return
        if 0 == self.returncode:
            self.status = status.SUCCESS
        else:
            self.status = status.FAIL

    def finalize(self, boot, args, data, resdir):
        wlog.info('Worker start to finalize...')
        if self.worker_obj:
            if self.worker_obj.finalize(boot=boot, args=args,data=data,resdir=resdir):
                self.finialized = True
                self.status = WorkerStatus.FINALIZED
            else:
                wlog.error("Error occurs when worker finalizing")
        elif not boot and not data:
            self.finialized = True
            self.status = WorkerStatus.FINALIZED
            return 0
        else:
            self.do_task(boot, args, data, resdir, flag='fin')

    def _checkLimit(self):
        duration = (datetime.datetime.now() - self.start_time).seconds
        if self.limit and duration >= self.limit:
            # Time out
            self.task_status = status.TIMEOUT
            self.kill()
            return False
        if self.virLimit and (self._getVirt() >= self.virLimit):
            # Memory overflow
            self.task_status = status.OVERFLOW
            self.kill()
            return False
        return True

    def kill(self):
        if not self.process:
            return
        import signal
        try:
            os.kill(self.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
        except:
            wlog.error('Error: error occurs when kill task, msg=%s',traceback.format_exc())

    def _getVirt(self):
        if not self.pid:
            return 0
        else:
            # TODO return GetVirUse(self.pid)
            return 0

    def _parseLog(self, data):
        result, self.fatalLine = self.logParser.parse(data)
        return result