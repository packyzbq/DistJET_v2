import Queue
import datetime
import json
import os,sys
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
        self.interval = Conf.Config.getCFGattr('HeartBeatInterval') if Conf.Config.getCFGattr('HeartBeatInterval') else 1
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
                        wlog.warning('[HeartBeatThread]: Reduplicated key=%s when build up heart beat message, skip it'%tmp_d.keys()[0])
                        continue
                    send_dict = dict(send_dict, **tmp_d)
                self.queue_lock.release()
                send_dict['Task'] = {}
                while not self.worker_agent.task_completed_queue.empty():
                    task = self.worker_agent.task_completed_queue.get()
                    send_dict['Task'] = dict(send_dict['Task'],**task)
                send_dict['uuid'] = self.worker_agent.uuid
                send_dict['wid'] = self.worker_agent.wid
                send_dict['health'] = self.worker_agent.health_info()
                send_dict['rTask'] = self.worker_agent.getRuntasklist()
                send_dict['ctime'] = time.time()
                # before send heartbeat, sync agent status
                self.worker_agent.status_lock.acquire()
                send_dict['wstatus'] = self.worker_agent.status
                self.worker_agent.status_lock.release()
                send_str = json.dumps(send_dict)
#                wlog.debug('[HeartBeat] Send msg = %s'%send_str)
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
        send_dict['Task'] = {}
        while not self.worker_agent.task_completed_queue.empty():
            task = self.worker_agent.task_completed_queue.get()
            send_dict['Task'] = dict(send_dict['Task'],**task)
        # add node health information
        send_dict['health'] = self.worker_agent.health_info()
        send_dict['ctime'] = time.time()
        #send_dict['wstatus'] = self.worker_agent.worker.status
        send_str = json.dumps(send_dict)
        wlog.debug('[HeartBeat] Send msg = %s'%send_str)
        ret = self._client.send_string(send_str, len(send_str), 0, Tags.MPI_PING)
        if ret != 0:
            #TODO add send error handler
            pass



    def set_ping_duration(self, interval):
        self.interval = interval

class WorkerAgent:
    """
    agent
    """
    def __init__(self, capacity=1):
        #multiprocessing.Process.__init__(self)
        #BaseThread.__init__(self,"agent")
        import uuid as uuid_mod
        self.uuid = str(uuid_mod.uuid4())
        global wlog
        wlog = logger.getLogger('Worker_%s'%self.uuid)
        self.worker_class=None

        self.recv_buff = IM.IRecv_buffer()
        self.__should_stop_flag = False
        Conf.Config()
        self.cfg = Conf.Config
        if self.cfg.isload():
            wlog.debug('[Agent] Loaded config file')
        wlog.debug('[Agent] Start to connect to service <%s>'%self.cfg.getCFGattr('svc_name'))
        self.client = Client(self.recv_buff, self.cfg.getCFGattr('svc_name'), self.uuid)
        ret = self.client.initial()
        if ret != 0:
            #TODO client initial error, add handler
            wlog.error('[Agent] Client initialize error, errcode = %d'%ret)
            #exit()

        self.task_add_acquire = False 
        self.wid = None
        self.appid = None   #The running app id
        self.capacity = capacity
        self.task_queue = Queue.Queue(maxsize=self.capacity)
        self.task_completed_queue = Queue.Queue()
        self.ignoreTask = []

        self.initExecutor = {}
        self.tmpLock = threading.RLock()
        self.finExecutor = {}
        self.fin_flag = False
        self.app_fin_flag = False # if agent has sent APP_FIN or not, avoid repeat sending
        self.haltflag=False
        # The operation/requirements need to transfer to master through heart beat
        self.heartcond = threading.Condition()
        self.heartbeat = HeartbeatThread(self.client,self,self.heartcond)

        self.status = WorkerStatus.NEW      # represent agent status, used for master management
        self.status_lock = threading.RLock()

        self.cond_list=[]
        self.worker_queue_list = []
        self.worker_list = []
        self.worker_status = {}
        #self.cond = threading.Condition()
        #self.worker = Worker(self,self.cond)
        #wlog.debug('[Agent] Start Worker Thread')
        #self.worker.start()

    def run(self):
        wlog.debug('[Agent] WorkerAgent run...')
        self.heartbeat.start()
        wlog.debug('[WorkerAgent] HeartBeat thread start...')
        while not self.__should_stop_flag:
            #if self.status == WorkerStatus.IDLE:
            #    time.sleep(Conf.Config.getCFGattr('Halt_Recv_Interval'))
            #else:
            time.sleep(0.5)
            if not self.recv_buff.empty():
                msg = self.recv_buff.get()
                if msg.tag == -1:
                    continue
                #wlog.debug('[Agent] Agent receive a msg = %s'%msg.sbuf[0:msg.size])
                recv_dict = json.loads(msg.sbuf[0:msg.size])
                for k,v in recv_dict.items():
                    # registery info v={wid:val,init:{boot:v, args:v, data:v, resdir:v}, appid:v, wmp:worker_module_path}
                    if int(k) == Tags.MPI_REGISTY_ACK:
                        self.status = WorkerStatus.RUNNING
                        if v.has_key('flag') and v['flag'] == 'NEWAPP':
                            wlog.debug('[WorkerAgent] Receive New App msg = %s' % v)
                            # Reset parameter
                            v['wid'] = self.wid
                            self.appid = v['appid']
                            self.task_queue.queue.clear()
                            self.task_completed_queue.queue.clear()
                            self.ignoreTask = []
                            self.initExecutor.clear()
                            self.finExecutor.clear()
                            self.fin_flag = False
                            self.app_fin_flag = False
                            self.haltflag = False
                            self.status = WorkerStatus.NEW
                        else:
                            wlog.debug('[WorkerAgent] Receive Registry_ACK msg = %s'%v)
                        #parse worker_module_path
                        worker_path = v['wmp']
                        if worker_path is not None and worker_path!='None':
                            module_path = os.path.abspath(worker_path)
                            sys.path.append(os.path.dirname(module_path))
                            worker_name = os.path.basename(module_path)
                            if worker_name.endswith('.py'):
                                worker_name = worker_name[:-3]
                            try:
                                worker_module = __import__(worker_name)
                                if worker_module.__dict__.has_key(worker_name) and callable(
                                        worker_module.__dict__[worker_name]):
                                    self.worker_class = worker_module.__dict__[worker_name]
                                    wlog.info('[Agent] Load specific worker class = %s' % self.worker_class)
                            except Exception:
                                wlog.error('[Agent] Error when import worker module %s, path = %s,errmsg=%s' % (
                                worker_name, worker_path, traceback.format_exc()))
                        else:
                            wlog.warning('[Agent] No specific worker input, use default')

                        try:
                            self.wid = v['wid']
                            self.appid = v['appid']
                            self.tmpLock.acquire()
                            self.iniExecutor = v['init']
                            self.tmpLock.release()

                            # notify worker initialize
                            wlog.info('[Agent] Start up worker and initialize')
                            for i in range(self.capacity):
                                self.cond_list.append(threading.Condition())
                                self.worker_list.append(
                                    Worker(i, self, self.cond_list[i], worker_class=self.worker_class))
                                wlog.debug('[Agent] Worker %s start' % i)
                                self.worker_list[i].start()

                            # notify the heartbeat thread
                            wlog.debug('[WorkerAgent] Wake up the heartbeat thread')
                            self.heartcond.acquire()
                            self.heartcond.notify()
                            self.heartcond.release()

                            #for i in range(len(self.worker_list)):
                            #    if self.worker_list[i].status == WorkerStatus.NEW:
                            #        self.cond_list[i].acquire()
                            #        self.cond_list[i].notify()
                            #        self.cond_list[i].release()
                        except KeyError:
                            pass
                    # add tasks v={tid:{boot:v, args:v, data:v, resdir:v}, tid:....}
                    elif int(k) == Tags.TASK_ADD:
                        self.status = WorkerStatus.RUNNING
                        self.haltflag=False
                        self.task_add_acquire = False
                        wlog.debug('[WorkerAgent] Receive TASK_ADD msg = %s'%v)
                        for tk,tv in v.items():
                            self.task_queue.put({tk:tv})
                            wlog.debug('[Agent] Add new task={%s:%s}'%(tk,tv))
                        #wlog.debug('[Agent] Worker Status = %d'%self.worker.status)
                        for i in range(len(self.worker_list)):
                            if self.worker_list[i].status == WorkerStatus.IDLE:
                                wlog.debug('[Agent] Worker %s IDLE, wake up worker'%i)
                                self.cond_list[i].acquire()
                                self.cond_list[i].notify()
                                self.cond_list[i].release()

                    # remove task, v=tid
                    elif int(k) == Tags.TASK_REMOVE:
                        wlog.debug('[WorkerAgent] Receive TASK_REMOVE msg = %s'%v)
                        self.tmpLock.acquire()
                        try:
                            for tid in v:
                                self.ignoreTask.append(tid)
                        finally:
                            self.tmpLock.release()
                        try:
                            for worker in self.worker_list:
                                if worker.running_task in v:
                                    t = worker.running_task
                                    worker.kill()
                                    self.ignoreTask.remove(t)
                        except ValueError:
                            wlog.warning('[Agent] Can not find tid %s when remove tasks'%t)
                    # master disconnect ack,
                    elif int(k) == Tags.LOGOUT:
                        wlog.debug('[WorkerAgent] Receive LOGOUT msg = %s' % v)
                        for i in range(len(self.worker_list)):
                            if self.worker_list[i].status == WorkerStatus.FINALIZED:
                                self.cond_list[i].acquire()
                                self.cond_list[i].notify()
                                self.cond_list[i].release()
                        # stop worker agent
                        #self.heartbeat.acquire_queue.put({Tags.LOGOUT_ACK:self.wid})
                        self.__should_stop_flag = True
                    elif int(k) == Tags.WORKER_STOP:
                        wlog.debug('[Agent] Receive WORKER_STOP msg = %s'%v)
                        for i in range(len(self.worker_list)):
                            if self.worker_list[i].status == WorkerStatus.RUNNING:
                                self.worker_list[i].terminate()
                            if self.worker_list[i].status == WorkerStatus.IDLE:
                                self.cond_list[i].acquire()
                                self.cond_list[i].notify()
                                self.cond_list[i].release()

                    # app finalize {boot:v, args:v, data:v, resdir:v}
                    elif int(k) == Tags.APP_FIN:
                        self.task_add_acquire = False
                        wlog.debug('[WorkerAgent] Receive APP_FIN msg = %s' % v)
                        self.tmpLock.acquire()
                        self.finExecutor = v
                        self.tmpLock.release()
                        self.fin_flag = True

                    elif int(k) == Tags.WORKER_HALT:
                        wlog.debug('[Agent] Receive WORKER_HALT command')
                        # check worker thread status
                        self.haltflag=True
                        wid = None
                        for worker in self.worker_list:
                            wlog.debug('[Agent-test] worker %s status = %s'%(worker.id,worker.status))
                            if worker.status == WorkerStatus.RUNNING:
                                wid = worker.id
                        if wid is None:
                            wlog.debug('[Agent] Worker has no more task to do, Agent change to IDLE')
                            self.status = WorkerStatus.IDLE
                        else:
                            wlog.debug('[Agent] Worker %s is running, Agent running'%wid)
                        self.task_add_acquire = False



            self.status_lock.acquire()
            # sync worker finalize status to agent and wait for worker join
            for worker in self.worker_list:
                if worker.status == WorkerStatus.FINALIZED:
                    while worker.is_alive():
                        worker.join()
                    self.worker_list.remove(worker)
                    try:
                        self.worker_status.pop(worker.id)
                    except:
                        pass
                    wlog.debug('[Agent] Remove Finalized worker %s, remains worker list=%s'%(worker.id,self.worker_list))
                else:
                    self.worker_status[worker.id]=worker.status
            if len(self.worker_list) == 0 and not self.app_fin_flag:
                self.status = WorkerStatus.FINALIZED
                self.haltflag=False
                self.heartbeat.acquire_queue.put({Tags.APP_FIN: {'wid': self.wid, 'recode': status.SUCCESS, 'result': None}})
                wlog.debug('[Agent] Send APP_FIN msg for logout/newApp')
                self.app_fin_flag = True
            wlog.debug('[Agent] Before My status = %s'%self.status)           
            # ask for new task
            if self.task_queue.qsize() < self.capacity and (not self.task_add_acquire) and (not self.fin_flag) and (self.status not in [WorkerStatus.NEW,WorkerStatus.FINALIZED]) and (not self.haltflag):
                wlog.debug('[Agent] Worker need more tasks, ask for new task')
                self.heartbeat.acquire_queue.put({Tags.TASK_ADD:self.capacity-self.task_queue.qsize()})
                self.task_add_acquire = True
            # sync worker running/idle status
            self.getStatus()
            wlog.debug('[Agent] After My status = %s'%self.status)

            # finalize worker
            if self.fin_flag and self.task_queue.empty():
                #wlog.debug('[Agent] Check worker finalize condition')
                flag = True
                for worker in self.worker_list:
                    if worker.status != WorkerStatus.IDLE:
                        wlog.debug('[Agent] Worker %s is running, cannot finalize, set finalize flag'%worker.id)
                        flag = False
                        break
                if flag:
                    for worker in self.worker_list:
                        if worker.finialized:
                            wlog.info('[Agent] Worker %s has finalized, ignore this message' % worker.id)
                            continue
                        worker.finialized = True
                        if worker.status == WorkerStatus.IDLE:
                            self.cond_list[worker.id].acquire()
                            self.cond_list[worker.id].notify()
                            self.cond_list[worker.id].release()
            self.status_lock.release() 
        wlog.debug('[Agent] Wait for worker thread join')
        for worker in self.worker_list:
            worker.join()
        wlog.info('[WorkerAgent] Worker thread has joined')
        self.stop()
        wlog.debug('[Agent] remains %d alive thread, [%s]'%(threading.active_count(), threading.enumerate()))

    def stop(self):
        self.__should_stop_flag = True
        if self.heartbeat:
            self.heartbeat.stop()
            self.heartbeat.join()
        ret = self.client.stop()
        wlog.info('[WorkerAgent] Agent stop..., exit code = %d'%ret)
        if ret != 0:
            wlog.error('[WorkerAgent] Client stop error, errcode = %d'%ret)
            # TODO add solution

    def getTask(self, workerid, status):
        if not self.task_queue.empty():
            if status in [WorkerStatus.INITILAZED,WorkerStatus.IDLE, WorkerStatus.RUNNING]:
                return self.task_queue.get()
            else:
                wlog.warning('[Agent] Worker attempt to get task in wrong status = %s'%status)
                return -1
        else:
            return None

    def getRuntasklist(self):
        rtask_list=[]
        for worker in self.worker_list:
            if worker.running_task is not None:
                rtask_list.append(worker.running_task)
        wlog.debug('[Agent] Running task = %s'%rtask_list)
        return rtask_list

    def getStatus(self):
        # sync worker status to agent
        for worker in self.worker_list:
            self.worker_status[worker.id]=worker.status
        cflag=True
        for k,v in self.worker_status.items():
            if v in [WorkerStatus.RUNNING, WorkerStatus.INITILAZED, WorkerStatus.FINALIZED]:
                wlog.debug('[Agent]Worker %s status = %s'%(k,v))
                cflag = False
                break
            if cflag:
                wlog.debug('[Agent] all worker idle')
                self.status = WorkerStatus.IDLE
        return self.status

    def task_done(self, tid, task_stat,**kwd):
        tmp_dict = dict({'task_stat':task_stat},**kwd)
        wlog.info('[Agent] Worker finish task %s, %s' % (tid,tmp_dict))
        self.task_completed_queue.put({tid:tmp_dict})
        if self.haltflag:
            wlog.debug('[Agent] Finish one task, ask for new task')
            self.heartbeat.acquire_queue.put({Tags.TASK_ADD:self.capacity-self.task_queue.qsize()})
                        
    def app_ini_done(self,workerid, returncode,errmsg=None, result=None):
        if returncode != 0:
            wlog.error('[Error] Worker %s initialization error, error msg = %s'%(workerid,errmsg))
            #TODO reinit worker
        else:
            self.status = WorkerStatus.INITILAZED
            self.worker_status[workerid] = WorkerStatus.INITILAZED
            wlog.debug('[Agent] Feed back app init result')
            self.heartbeat.acquire_queue.put({Tags.APP_INI:{'wid':self.wid,'recode':returncode, 'errmsg':errmsg, 'result':result}})

    def app_fin_done(self, workerid, returncode, errmsg = None, result=None):
        #TODO wait for all worker finalize, then send App_Fin
        if returncode != 0:
            wlog.error('[Error] Worker %s finalization error, error msg = %s'%(workerid,errmsg))
        else:
            #self.status = WorkerStatus.FINALIZED
            self.worker_status[workerid] = WorkerStatus.FINALIZED
            wlog.debug('[Agent] Worker_%s finalized'%workerid)
            #self.heartbeat.acquire_queue.put({Tags.APP_FIN:{'wid':self.wid,'recode':returncode, 'result':result}})



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


class WorkerProcess(multiprocessing.Process):
    def __init__(self,id, workeragent, cond, name="", worker_class=None):
        name = '.'.join(['DistJET.Worker_%s'%self.ident, name])
        super(multiprocessing.Process,self).__init__(name=name)
        self.id = id
        self.workeragent = workeragent
        self.running_task = None
        self.cond = cond
        self.initialized = False
        self.finialized = False
        self.status = WorkerStatus.NEW
        self.daemon = True
        self.__should_stop = False
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
        self.limit = None
        self.virLimit = None
        self.start_time = None
        self.end_time = None

        self.lock = multiprocessing.RLock()

    def run(self):
        while not self.__should_stop:
            while not self.initialized:
                wlog.debug('[Worker] Worker Start and not initial, ready to sleep')
                self.status = WorkerStatus.NEW
                self.cond.acquire()
                self.cond.wait()
                self.cond.release()

                if self.finialized:
                    break

                self.workeragent.tmpLock.acquire()
                if set(['boot','data','args','resdir']).issubset(set(self.workeragent.iniExecutor.keys())):
                    self.initialize(self.workeragent.iniExecutor['boot'], self.workeragent.iniExecutor['args'], self.workeragent.iniExecutor['data'], self.workeragent.iniExecutor['resdir'])
                else:
                    wlog.warning('[Worker] Worker cannot initialize beacuse of lack of data, need keys = boot,data,args,resdir,your key = %s' % (self.workeragent.iniExecutor.keys()))
                self.workeragent.tmpLock.release()
                if not self.initialized:
                    continue
            if self.finialized:
                wlog.info('[Worker] Initialize error, worker terminate')
                break
            wlog.info('[Worker] Initialized, Ready to running tasks')
            while not self.finialized:
                if not self.workeragent.task_queue.empty():
                    task = self.workeragent.task_queue.get()
                    if task in self.workeragent.ignoreTask:
                        self.workeragent.ignoreTask.remove(task.keys()[0])
                        continue
                    # TODO add task failed handle
                    self.status = WorkerStatus.RUNNING
                    tid,v = task.popitem()
                    self.do_task(v['boot'],v['args'],v['data'],v['resdir'], tid=int(tid))
                    self.workeragent.task_done(tid, self.task_status, time_start=self.start_time, time_fin=self.end_time,errcode=self.returncode)
                    self.running_task = None
                else:
                    self.status = WorkerStatus.IDLE
                    wlog.info('Worker: No task to do, worker status = %d'%self.status)
                    self.cond.acquire()
                    self.cond.wait()
                    self.cond.release()
                    wlog.debug('[Worker] I am wake')
            # do finalize
            # TODO according to Policy ,decide to force finalize or wait for all task done
            while self.status != WorkerStatus.FINALIZED:
                self.workeragent.tmpLock.acquire()
                self.finalize(self.workeragent.finExecutor['boot'], self.workeragent.finExecutor['args'],
                              self.workeragent.finExecutor['data'], self.workeragent.finExecutor['resdir'])
                self.workeragent.tmpLock.release()
                self.cond.acquire()
                self.cond.wait()
                self.cond.release()
                wlog.debug('[Worker] I am wake up, ready to stop')
            self.stop()
            wlog.debug('[Worker] Remains %d thread alive, [%s]'%(threading.active_count(), threading.enumerate()))


# FIXME: worker initial in one process, cannot run task in other process. Should make init/run/finalize in the same process
# Load customed worker class  or  make the task script include init-do-fin steps
class Worker(BaseThread):
    def __init__(self, id, workagent, cond, name=None, worker_class=None):
        if not name:
            name = "worker_%s"%id
        BaseThread.__init__(self,name)
        self.workeragent = workagent
        self.id = id
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
            wlog.debug('[Worker_%s] Create Worker object %s'%(self.id,self.worker_obj.__class__.__name__))

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
        self.end_time = None

        self.lock = threading.RLock()


    def run(self):
        while not self.get_stop_flag():
            while not self.initialized:
                wlog.debug('[Worker] Worker Start and not initial')
                #self.status = WorkerStatus.NEW
                #self.cond.acquire()
                #self.cond.wait()
                #self.cond.release()

                if self.finialized:
                    break

                self.workeragent.tmpLock.acquire()
                if set(['boot','data','args','resdir']).issubset(set(self.workeragent.iniExecutor.keys())):
                    self.initialize(self.workeragent.iniExecutor['boot'], self.workeragent.iniExecutor['args'], self.workeragent.iniExecutor['data'], self.workeragent.iniExecutor['resdir'])
                else:
                    wlog.warning('[Worker_%s] Worker cannot initialize beacuse of lack of data, need keys = boot,data,args,resdir,your key = %s' % (self.id,self.workeragent.iniExecutor.keys()))
                self.workeragent.tmpLock.release()
                if not self.initialized:
                    continue
            if self.finialized:
                wlog.info('[Worker_%s] Initialize error, worker terminate'%self.id)
                break
            wlog.info('[Worker_%s] Initialized, Ready to running tasks'%self.id)
            while not self.finialized:
                task = self.workeragent.getTask(self.id, self.status)
                if task and task != -1:
                    if task.keys()[0] in self.workeragent.ignoreTask:
                        self.workeragent.tmpLock.acquire()
                        try:
                            self.workeragent.ignoreTask.remove(task.keys()[0])
                        finally:
                            self.workeragent.tmpLock.release()
                        continue
                    self.status = WorkerStatus.RUNNING
                    tid, v = task.popitem()
                    self.do_task(v['boot'], v['args'], v['data'], v['resdir'], tid=int(tid))
                    self.workeragent.task_done(tid, self.task_status, time_start=self.start_time,
                                               time_fin=self.end_time, errcode=self.returncode)
                    self.running_task = None
                elif task == -1:
                    pass
                else:
                    self.status = WorkerStatus.IDLE
                    wlog.info('[Worker_%s]: No task to do, worker status = %d'%(self.id,self.status))
                    self.cond.acquire()
                    self.cond.wait()
                    self.cond.release()
                    wlog.debug('[Worker_%s] I am wake'%self.id)
            # do finalize
            # TODO according to Policy ,decide to force finalize or wait for all task done
            while self.status != WorkerStatus.FINALIZED:
                self.workeragent.tmpLock.acquire()
                self.finalize(self.workeragent.finExecutor['boot'], self.workeragent.finExecutor['args'],
                              self.workeragent.finExecutor['data'], self.workeragent.finExecutor['resdir'])
                self.workeragent.tmpLock.release()
                # TODO because ignore finalize error, worker stop after finalizing without wait()
                #self.cond.acquire()
                #self.cond.wait()
                #self.cond.release()
                wlog.debug('[Worker_%s] I am wake up, ready to stop'%self.id)
            self.stop()
            wlog.debug('[Worker_%s] Remains %d thread alive, [%s]'%(self.id,threading.active_count(), threading.enumerate()))
            #self.workeragent.app_fin_done(self.returncode, status.describe(self.returncode))


            # sleep or stop
            #self.cond.acquire()
            #self.cond.wait()
            #self.cond.release()





    def initialize(self, boot, args, data, resdir, **kwargs):
        wlog.info('[Worker_%s] Start to initialize...'%self.id)
        if self.worker_obj:
            self.returncode = self.worker_obj.initialize(boot=boot, args=args,data=data,resdir=resdir)
            if self.returncode == 0:
                wlog.debug('[Worker_%s] Worker obj %s initialize'%(self.id,self.worker_obj.__class__.__name__))
                self.lock.acquire()
                self.initialized = True
                self.status = WorkerStatus.INITILAZED
                self.lock.release()
            else:
                wlog.error('[Worker_%s]Error: error occurs when initializing worker'%self.id)
        elif not boot and not data:
            self.lock.acquire()
            self.initialized = True
            self.status = WorkerStatus.INITILAZED
            self.returncode = status.SUCCESS
            self.lock.release()
        else:
            self.returncode = self.do_task(boot, args, data, resdir, flag='init')
            if self.returncode == status.SUCCESS:
                self.lock.acquire()
                self.initialized = True
                self.status = WorkerStatus.INITILAZED
                self.lock.release()
        #recode = status.SUCCESS if self.initialized else status.FAIL
        # TODO add result
        # ignore initial error
        self.status=WorkerStatus.INITILAZED
        self.initialized = True
        self.workeragent.app_ini_done(self.id, status.SUCCESS, status.describe(self.returncode))

    def do_task(self, boot, args, data, resdir,tid=0,shell=False, extra={}):
        self.running_task = tid
        self.start_time = time.time()
        #TODO modify task log name
        logFile = open('%s/app_%d_task_%d' % (resdir, self.workeragent.appid, tid), 'w+')
        if self.worker_obj:
            wlog.info('[Worker_%s] running task %s' % (self.id, self.running_task))
            self.returncode = self.worker_obj.do_work(boot=boot,args=args,data=data,resdir=resdir,extra=extra, log=logFile)
            self.end_time = time.time()
            wlog.info('[Worker_%s] Task %d have finished, returncode = %s, start in %s, end in %s' % (self.id, tid, self.returncode,time.strftime("%H:%M:%S", time.localtime(self.start_time)),time.strftime("%H:%M:%S", time.localtime(self.end_time))))
        else:
            self.task_status = None
            executable = []
            # TODO if boot has more than one bash script
            if boot[0].endswith('.sh') or shell:
                executable.append("bash")
            elif boot[0].endswith('.py') or not shell:
                executable.append("python")
            executable.append(boot[0])
            if len(args) > 0:
                for k,v in args.items():
                    executable.append(str(k))
                    if v:
                        executable.append(str(v))
            if len(data) > 0:
                for k,v in data.items():
                    executable.append(str(k))
                    if v :
                        executable.append(str(v))
            self.process = subprocess.Popen(executable, stdout=self.stdout, stderr=subprocess.STDOUT, shell=shell)
            self.pid = self.process.pid
            wlog.info('[Worker_%s] Pid %s run task, %s'%(self.id,self.pid,executable))
            if len(resdir) and not os.path.exists(resdir):
                os.makedirs(resdir)
            #if kwd.has_key('flag'):
            #    logFile = open('%s/%s_log'%(resdir,kwd['flag']),'w+')
            #else:

            while True:
                fs = select.select([self.process.stdout], [], [], Conf.Config.getCFGattr('AppRespondTimeout'))
                if not fs[0]:
                    wlog.info('[Worker_%s] The task has no respond, kill the task'%self.id)
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
                            wlog.error('[Worker_%s] Error occurs when run task, stop running'%self.id)
                            self.task_status = status.FAIL
                            self.kill()
                            break
                    if not self._checkLimit():
                        break
            self.returncode = self.process.wait()
            self.end_time = time.time()
            wlog.info('[Worker_%s] Task %d have finished, start in %s, end in %s'%(self.id,tid,time.strftime("%H:%M:%S",time.localtime(self.start_time)),time.strftime("%H:%M:%S",time.localtime(self.end_time))))
        self.running_task = None
        logFile.write('-'*50+'\n')
        logFile.write('start time : %s\n'%time.strftime('%H:%M:%S',time.localtime(self.start_time)))
        logFile.write('end time : %s\n'%time.strftime('%H:%M:%S',time.localtime(self.end_time)))
        logFile.flush()
        logFile.close()
        if self.task_status:
            return
        if 0 == self.returncode:
            self.task_status = status.SUCCESS
        else:
            self.task_status = status.FAIL

    def finalize(self, boot, args, data, resdir):
        wlog.info('[Worker_%s] start to finalize...'%self.id)
        if self.worker_obj:
            self.returncode = self.worker_obj.finalize(boot=boot, args=args,data=data,resdir=resdir)
            if self.returncode == 0:
                self.lock.acquire()
                #self.finialized = True
                self.status = WorkerStatus.FINALIZED
                self.lock.release()
            else:
                wlog.error("[Worker_%s]Error occurs when worker finalizing"%self.id)
        elif not boot and not data:
            self.lock.acquire()
            #self.finialized = True
            self.status = WorkerStatus.FINALIZED
            self.lock.release()
            wlog.debug('[Worker_%s] Worker has finalized'%self.id)
            self.returncode = status.SUCCESS
            
        else:
            self.returncode = self.do_task(boot, args, data, resdir, flag='fin')
            if self.returncode == status.SUCCESS:
                self.lock.acquire()
                #self.finialized = True
                self.status = WorkerStatus.FINALIZED
                self.lock.release()
        # TODO add finalize result record
        # ignore finalize error
        self.status = WorkerStatus.FINALIZED
        self.workeragent.app_fin_done(self.id, status.SUCCESS,status.describe(self.returncode))

    def _checkLimit(self):
        duration = time.time() - self.start_time
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

    def terminate(self):
        self.lock.acquire()
        self.finialized = True
        self.status = WorkerStatus.FINALIZED
        self.lock.release()

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
