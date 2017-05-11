import Queue
import json
import subprocess
import os
import select
import threading
import datetime, time
import ConfigParser
import sys
import traceback
import logging

import IRecv_Module as IM
from MPI_Wrapper import Tags ,Client

from BaseThread import BaseThread
from Conf import Policy
from Task import TaskStatus
from WorkerRegistry import WorkerStatus

log = logging.getLogger('WorkerAgent')
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
        send_dict[Tags.MPI_REGISTY] = {'uuid':self.worker_agent.uuid,'capacity':self.worker_agent.capacity}
        send_dict['ctime'] = datetime.datetime.now()
        send_str = json.dumps(send_dict)
        self._client.send_string(send_str, len(send_str),0,Tags.MPI_PING)

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
                send_dict['ctime'] = datetime.datetime.now()

                send_str = json.dumps(send_dict)
                self._client.send_string(send_str, len(send_str), 0, Tags.MPI_PING)
            except Exception:
                wlog.error('[HeartBeatThread]: unkown error, thread stop. msg=%s', traceback.format_exc())
            else:
                time.sleep(self.interval)

        # TODO add the last time to ping Master

    def set_ping_duration(self, interval):
        self.interval = interval

class WorkerAgent:
    """
    agent
    """
    def __init__(self,svcname, capacity=1):
        self.recv_buff = IM.IRecv_buffer()
        self.__should_stop_flag = False
        import uuid as uuid_mod
        self.uuid = str(uuid_mod.uuid4())
        self.client = Client(self.recv_buff, svcname, self.uuid)
        self.client.initial()

        self.wid = None
        self.appid = None   #The running app id
        self.capacity = capacity
        self.task_queue = Queue.Queue(maxsize=self.capacity)
        self.task_completed_queue = Queue.Queue()

        self.tmpExecutor = {}
        self.tmpLock = threading.RLock()

        self.wait_to_ping = {}      # The operation/requirements need to transfer to master through heart beat
        self.heartbeat = HeartbeatThread(self.client,self)

        self.ignoreTask = []

        self.cond = threading.Condition()
        self.worker = Worker()

    def start(self):
        self.heartbeat.run()
        while True:
            if not self.recv_buff.empty():
                recv_dict = json.loads(self.recv_buff.get())
                for k,v in recv_dict:
                    # registery info v={wid:val,init:{boot:v, args:v, data:v, resdir:v}, appid:v}
                    if k == Tags.MPI_REGISTY:
                        try:
                            self.wid = v['wid']
                            self.appid = v['appid']
                            self.tmpLock.acquire()
                            self.tmpExecutor = v['init']
                            self.tmpLock.release()
                        except KeyError:
                            pass
                    # add tasks v={tid:{boot:v, args:v, data:v, resdir:v}, tid:....}
                    elif k == Tags.TASK_ADD:
                        for tk,tv in v:
                            self.task_queue.put({tk,tv})
                        if self.worker.status == WorkerStatus.IDLE:
                            self.cond.acquire()
                            self.cond.notify()
                            self.cond.release()

                    # remove task, v=tid
                    elif k == Tags.TASK_REMOVE:
                        if self.worker.running_task == v:
                            self.worker.kill()
                        else:
                            self.ignoreTask.append(v)
                    # master disconnect ack,
                    elif k == Tags.LOGOUT_ACK:
                        # TODO





    def _app_change(self):
        """
        WorkerAgent calls when new app comes.
        :return:
        """
        #TODO
        pass

    def health_info(self):
        """
        Provide node health information which is transfered to Master
        :return:
        """
        #TODO
        pass

    def task_completion_info(self):
        """
        Provide the completed tasks with Master
        :return:
        """
        # TODO
        pass

class Worker(BaseThread):
    def __init__(self, workagent, cond, cfg):
        BaseThread.__init__(self,"worker")
        self.workeragent = workagent
        self.running_task = None
        self.cond = cond
        self.initialized = False
        self.finialized = False
        self.status = WorkerStatus.NEW

        self.cfg= None
        self.log = wlog
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
                    self.do_task(task.boot,task.args, task.data, task.res_dir,task.tid)
                    self.workeragent.task_completed_queue.put(task)

                wlog.info('Worker: No task to do, Idle...')
                self.status = WorkerStatus.IDLE
                self.cond.acquire()
                self.cond.wait()
                self.cond.release()
            # do finalize
            self.workeragent.tmpLock.acquire()
            self.finalize(self.workeragent.tmpExecutor['boot'], self.workeragent.tmpExecutor['args'],
                          self.workeragent.tmpExecutor['data'], self.workeragent.tmpExecutor['resdir'])
            self.workeragent.tmpLock.release()


            # sleep or stop
            self.cond.acquire()
            self.cond.wait()
            self.cond.release()





    def initialize(self, boot, args, data, resdir):
        wlog.info('Worker start to initialize...')
        if not boot and not data:
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
        if kwd.has_key('flag') and kwd['flag'] == 'init':
            logFile = open('Init_log','w+')
        else:
            logFile = open('app_%d_task_%d'%(self.workeragent.appid,tid), 'w+')
        while True:
            fs = select.select([self.process.stdout],[],[],self.cfg.timeout)
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

    def finalize(self, boot, args, data, resdir):
        wlog.info('Worker start to finalize...')
        if not boot and not data:
            self.finialized = True
            self.status = WorkerStatus.INITILAZED
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