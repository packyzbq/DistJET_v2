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

import IR_Buffer_Module as IM

import HealthDetect as HD
from BaseThread import BaseThread
from MPI_Wrapper import Tags ,Client
from Util import logger
from WorkerRegistry import WorkerStatus
from python.Util import Conf

#log = logger.getLogger('WorkerAgent')
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
    def __init__(self, client, worker_agent, cond):
        BaseThread.__init__(self, name='HeartbeatThread')
        self._client = client
        self.worker_agent = worker_agent
        self.queue_lock = threading.RLock()
        self.acquire_queue = Queue.Queue()         # entry = key:val
        self.interval = 1
        self.cond = cond
        global wlog

    def run(self):
        #add first time to ping master
        send_dict = {}
        send_dict['flag'] = 'firstPing'
        send_dict[Tags.MPI_REGISTY] = {'capacity':self.worker_agent.capacity}
        send_dict['ctime'] = time.time()
        send_dict['uuid'] = self.worker_agent.uuid
        send_str = json.dumps(send_dict)
        wlog.debug('[HeartBeat] Send msg = %s'%send_str)
        ret = self._client.send_string(send_str, len(send_str),0,Tags.MPI_REGISTY)
        if ret != 0:
            #TODO send error,add handler
            pass

        # wait for the wid and init msg from master
        self.cond.acquire()
        self.cond.wait()
        self.cond.release()

        while not self.get_stop_flag():
            try:
                self.queue_lock.acquire()
                send_dict.clear()
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
                send_dict['ctime'] = time.time()
                send_str = json.dumps(send_dict)
                wlog.debug('[HeartBeat] Send msg = %s'%send_str)
                ret = self._client.send_string(send_str, len(send_str), 0, Tags.MPI_PING)
                if ret != 0:
                    #TODO add send error handler
                    pass
            except Exception:
                wlog.error('[HeartBeatThread]: unkown error, thread stop. msg=%s', traceback.format_exc())
                break
            else:
                time.sleep(self.interval)

        # the last time to ping Master
        if not self.acquire_queue.empty():
            remain_command = ''
            while not self.acquire_queue.empty():
                remain_command+=self.acquire_queue.get().keys()
            wlog.waring('[HeartBeat] Acquire Queue has more command, %s, ignore them'%remain_command)
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
        send_dict['ctime'] = time.time()
        send_str = json.dumps(send_dict)
        wlog.debug('[HeartBeat] Send msg = %s'%send_str)
        ret = self._client.send_string(send_str, len(send_str), 0, Tags.MPI_DISCONNECT)
        if ret != 0:
            #TODO add send error handler
            pass



    def set_ping_duration(self, interval):
        self.interval = interval

class WorkerAgent:
    """
    agent
    """
    def __init__(self,cfg_path=None, capacity=1):
        #multiprocessing.Process.__init__(self)
        #BaseThread.__init__(self,"agent")
        if not cfg_path or cfg_path=='null':
            #use default path
            cfg_path = os.getenv('DistJETPATH')+'/config/default.cfg'
        Conf.set_inipath(cfg_path)
        global wlog
        self.recv_buff = IM.IRecv_buffer()
        self.__should_stop_flag = False
        import uuid as uuid_mod
        self.uuid = str(uuid_mod.uuid4())
        wlog = logger.getLogger('Worker_%s'%self.uuid)
        Conf.Config()
        self.cfg = Conf.Config
        if self.cfg.isload():
            wlog.debug('[Agent] Loaded config file %s'%cfg_path)
        wlog.debug('[Agent] Start to connect to service <%s>'%self.cfg.getCFGattr('svc_name'))
        self.client = Client(self.recv_buff, self.cfg.getCFGattr('svc_name'), self.uuid)
        ret = self.client.initial()
        if ret != 0:
            #TODO client initial error, add handler
            wlog.error('[Agent] Client initialize error, errcode = %d'%ret)
            #exit()

        
        self.wid = None
        self.appid = None   #The running app id
        self.capacity = capacity
        self.task_queue = Queue.Queue(maxsize=self.capacity)
        self.task_completed_queue = Queue.Queue()

        self.tmpExecutor = {}
        self.tmpLock = threading.RLock()

        # The operation/requirements need to transfer to master through heart beat
        self.heartcond = threading.Condition()
        self.heartbeat = HeartbeatThread(self.client,self,self.heartcond)

        self.ignoreTask = []
        self.cond = threading.Condition()
        self.worker = Worker(self,self.cond)
        wlog.debug('[Agent] Start Worker Thread')
        self.worker.start()

    def run(self):
        wlog.debug('[Agent] WorkerAgent run...')
        self.heartbeat.start()
        wlog.debug('[WorkerAgent] HeartBeat thread start...')
        while True:
            time.sleep(1)
            if not self.recv_buff.empty():
                msg = self.recv_buff.get()
                if msg.tag == -1:
                    continue
                wlog.debug('[Agent] Agent receive a msg = %s'%msg.sbuf[0:msg.size])
                recv_dict = json.loads(msg.sbuf[0:msg.size])
                for k,v in recv_dict.items():
                    # registery info v={wid:val,init:{boot:v, args:v, data:v, resdir:v}, appid:v}
                    if int(k) == Tags.MPI_REGISTY_ACK:
                        wlog.debug('[WorkerAgent] Receive Registry_ACK msg = %s'%v)
                        try:
                            self.wid = v['wid']
                            self.appid = v['appid']
                            self.tmpLock.acquire()
                            self.tmpExecutor = v['init']
                            self.tmpLock.release()
                            # notify the heartbeat thread
                            wlog.debug('[WorkerAgent] Wake up the heartbeat thread')
                            self.heartcond.acquire()
                            self.heartcond.notify()
                            self.heartcond.release()
                        except KeyError:
                            pass
                    # add tasks v={tid:{boot:v, args:v, data:v, resdir:v}, tid:....}
                    elif int(k) == Tags.TASK_ADD:
                        wlog.debug('[WorkerAgent] Receive TASK_ADD msg = %s'%v)
                        for tk,tv in v:
                            self.task_queue.put({tk:tv})
                        if self.worker.status == WorkerStatus.IDLE:
                            self.cond.acquire()
                            self.cond.notify()
                            self.cond.release()

                    # remove task, v=tid
                    elif int(k) == Tags.TASK_REMOVE:
                        wlog.debug('[WorkerAgent] Receive TASK_REMOVE msg = %s'%v)
                        if self.worker.running_task == v:
                            self.worker.kill()
                        else:
                            self.ignoreTask.append(v)
                    # master disconnect ack,
                    elif int(k) == Tags.LOGOUT:
                        wlog.debug('[WorkerAgent] Receive LOGOUT msg = %s' % v)
                        if self.worker.status != WorkerStatus.FINALIZED:
                            wlog.error('logout error because of wrong worker status, worker status = %d', self.worker.status)
                            # TODO do something
                        self.cond.acquire()
                        self.cond.notify()
                        self.cond.release()
                        # stop worker agent
                        #self.heartbeat.acquire_queue.put({Tags.LOGOUT_ACK:self.wid})
                        break
                    # app finalize {fin:{boot:v, args:v, data:v, resdir:v}}
                    elif int(k) == Tags.APP_FIN:
                        wlog.debug('[WorkerAgent] Receive APP_FIN msg = %s' % v)
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
                        wlog.debug('[WorkerAgent] Receive NEW_APP msg = %s' % v)
                        # TODO new app arrive, need refresh
                        pass
            if self.task_queue.qsize() < self.capacity and self.worker.status == WorkerStatus.INITILAZED:
                self.heartbeat.acquire_queue.put({Tags.TASK_ADD:self.capacity-self.task_queue.qsize()})

        self.worker.join()
        wlog.info('[WorkerAgent] Worker thread has joined')
        self.stop()

    def stop(self):
        wlog.info('[WorkerAgent] Agent stop...')
        self.__should_stop_flag = True
        if self.heartbeat:
            self.heartbeat.stop()
            self.heartbeat.join()
        ret = self.client.stop()
        if ret != 0:
            wlog.error('[WorkerAgent] Client stop error, errcode = %d'%ret)
            # TODO add solution

    def task_done(self, tid, task_stat,**kwd):
        tmp_dict = dict({'task_stat':task_stat},**kwd)
        self.task_completed_queue.put({tid:tmp_dict})

    def app_ini_done(self,returncode,errmsg=None):
        if returncode != 0:
            wlog.error('[Error] Worker initialization error, error msg = %s',errmsg)
        self.heartbeat.acquire_queue.put({Tags.APP_INI:{'wid':self.wid,'recode':returncode, 'errmsg':errmsg}})

    def app_fin_done(self, returncode, errmsg = None):
        if returncode != 0:
            wlog.error('[Error] Worker finalization error, error msg = %s',errmsg)
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
        global wlog
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
                wlog.debug('[Worker] Worker Start and not initial, ready to sleep')
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
                    self.workeragent.task_done(tid, self.task_status, time_start=self.start_time, time_fin=time.time(),errcode=self.returncode)
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
        self.start_time = time.time()
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
        duration = (time.time() - self.start_time).seconds
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
