import Queue
import datetime
import json
import time
import os

from Util import logger

control_log = logger.getLogger('Control_Log')
master_log = logger.getLogger('Master')

import IR_Buffer_Module as IM
import IScheduler
import WorkerRegistry
WorkerRegistry.wRegistery_log = master_log
import MPI_Wrapper
MPI_Wrapper.MPI_log = master_log

from BaseThread import BaseThread
from IAppManager import SimpleAppManager
from Task import TaskStatus
from python.Util.Conf import Config,set_inipath
from WorkerAgent import status


def MSG_wrapper(**kwd):
    return json.dumps(kwd)

class ControlThread(BaseThread):
    """
    monitor the worker registry to manage worker
    """
    def __init__(self, master):
        BaseThread.__init__(self,'ControlThread')
        self.master = master
        self.processing = False

    def run(self):
        time_start = time.time()
        control_log.info('Control Thread start...')
        while not self.get_stop_flag():
            try:
                for wid in self.master.worker_registry:
                    w = self.master.worker_registry.get(wid)
                    try:
                        w.alive_lock.acquire()
                        if w.lost():
                            # lost worker
                            control_log.warning('lost worker: %d', wid)
                            self.master.remove_worker(wid)
                            continue
                        if w.alive:
                            if w.worker_status == WorkerRegistry.WorkerStatus.RUNNING:
                                w.idle_time = 0
                            else:
                                if w.idle_time == 0:
                                    w.idle_time = time.time()
                            if w.isIdle_timeout():
                                # idle timeout, worker will be removed
                                control_log.warning('worker %d idle too long and will be removed', wid)
                                # TODO notify worker stop
                                self.master.remove_worker(wid)
                    finally:
                        w.alive_lock.release()
            finally:
                pass

            time.sleep(self.master.cfg.getPolicyattr('CONTROL_DELAY'))

    def activateProcessing(self):
        self.processing = True

class IJobMaster:
    """
    interface used by Scheduler to control task scheduling
    """
    pass

class JobMaster(IJobMaster):
    def __init__(self, cfg_path = None, applications=None):
        if not cfg_path:
            #use default path
            cfg_path = os.getenv('DistJETPATH')+'/config/default.cfg'
        set_inipath(cfg_path)
        self.cfg = Config()
        self.svc_name = self.cfg.getCFGattr('svc_name')
        if not self.svc_name:
            self.svc_name = 'Default'
        # worker list
        self.worker_registry = WorkerRegistry.WorkerRegistry()
        # scheduler
        self.task_scheduler = None
        self.appmgr = None

        self.recv_buffer = IM.IRecv_buffer()

        self.control_thread = ControlThread(self)

        self.applications = applications       # list

        self.command_q = Queue.Queue()

        self.__wid = 1

        # TODO(optional) load customed AppManager
        self.appmgr = SimpleAppManager(apps=self.applications)
        master_log.debug('[Master] Appmgr has instanced')
        if not self.appmgr.runflag:
            # appmgr load task error, exit
            self.stop()
            return
        #self.task_scheduler = IScheduler.SimpleScheduler(self.appmgr,self.worker_registry)
        if self.appmgr.get_current_app():
            self.task_scheduler = self.appmgr.get_current_app().scheduler(self.appmgr, self.worker_registry)
        else:
            self.task_scheduler = IScheduler.SimpleScheduler(self.appmgr,self.worker_registry)
        self.task_scheduler.appid = self.appmgr.get_current_appid()

        self.server = MPI_Wrapper.Server(self.recv_buffer,self.svc_name)
        ret = self.server.initialize()
        if ret != 0:
            master_log.error('[Master] Server initialize error, stop. errcode = %d'%ret)
            # TODO add error handler
            #exit()

        self.__stop = False

    def stop(self):
        if self.task_scheduler and self.task_scheduler.isAlive():
            self.task_scheduler.join()
            master_log.info('[Master] TaskScheduler has joined')
        if self.control_thread and self.control_thread.isAlive():
            self.control_thread.stop()
            self.control_thread.join()
            master_log.info('[Master] Control Thread has joined')
        ret = self.server.stop()
        if ret != 0:
            master_log.error('[Master] Server stop error, errcode = %d'%ret)
            # TODO add solution
        else:
            master_log.info('[Master] Server stop')
        self.__stop = True

    def register_worker(self, w_uuid, capacity = 1):
        master_log.debug('[Master] register worker %s'%w_uuid)
        worker = self.worker_registry.add_worker(w_uuid, capacity)
        if not worker:
            master_log.warning('[Master] The uuid=%s of worker has already registered', w_uuid)
        else:
            send_dict = {'wid':worker.wid, 'appid':self.task_scheduler.appid, 'init':self.task_scheduler.init_worker(), 'tag': MPI_Wrapper.Tags.MPI_REGISTY_ACK, 'uuid':w_uuid} 
            self.command_q.put(send_dict)
            #send_str = MSG_wrapper(wid=worker.wid,appid=self.task_scheduler.appid, init=self.task_scheduler.init_worker())
            #self.server.send_string(send_str, len(send_str), w_uuid, MPI_Wrapper.Tags.MPI_REGISTY_ACK)

    def remove_worker(self,wid):
        self.worker_registry.remove_worker(wid)
        self.task_scheduler.worker_removed(wid, time.time())

    def anaylize_health(self, info):
        #TODO give a threshold of the health of a node
        pass

    def startProcessing(self):
                # TODO add start worker command

        while not self.__stop:
            if not self.recv_buffer.empty():
                msg = self.recv_buffer.get()
                if msg.tag != -1:
                    master_log.debug('[Master] Receive msg = %s' % msg.sbuf[0:msg.size])
                    recv_dict = json.loads(msg.sbuf[0:msg.size])
                    if recv_dict.has_key('flag'):
                        if recv_dict['flag'] == 'firstPing' and msg.tag == MPI_Wrapper.Tags.MPI_REGISTY:
                            # register worker
                            # check dict's integrity
                            if self.check_msg_integrity('firstPing', recv_dict):
                                master_log.debug('[Master] Receive REGISTY msg = %s'%recv_dict)
                                self.register_worker(recv_dict['uuid'], recv_dict[str(MPI_Wrapper.Tags.MPI_REGISTY)]['capacity'])
                            else:
                                master_log.error('firstPing msg incomplete, key=%s'%recv_dict.keys())
                            #self.command_q.put({MPI_Wrapper.Tags.APP_INI: self.appmgr.get_app_init(self.appmgr.current_app_id), 'uuid':recv_dict['uuid']})
                        elif recv_dict['flag'] == 'lastPing' and msg.tag == MPI_Wrapper.Tags.MPI_DISCONNECT:
                            # last ping from worker, sync completed task, report node's health, logout and disconnect worker
                            master_log.debug('[Master] Receive DISCONNECT msg = %s' % recv_dict)
                            for tid, val in recv_dict['Task']:
                                # handle complete task
                                if self.check_msg_integrity('Task',val):
                                    if val['task_stat'] == TaskStatus.COMPLETED:
                                        self.task_scheduler.task_completed(recv_dict['wid'], tid, val['time_start'], val['time_fin'])
                                    elif val['task_stat'] == TaskStatus.FAILED:
                                        self.task_scheduler.task_failed(recv_dict['wid'], tid, val['time_start'], val['time_fin'],val['errcode'])
                                else:
                                    master_log.error('Task msg incomplete, key=%s'%val.keys())
                            # logout and disconnect worker
                            self.remove_worker(recv_dict['wid'])

                    # normal ping msg
                    elif recv_dict.has_key('Task'):
                        # handle complete tasks
                        for tid, val in recv_dict['Task'].items():
                            # handle complete task
                            if self.check_msg_integrity('Task',val):
                                if val['task_stat'] == TaskStatus.COMPLETED:
                                    self.task_scheduler.task_completed(recv_dict['wid'], tid, val['time_start'], val['time_fin'])
                                elif val['task_stat'] == TaskStatus.FAILED:
                                    self.task_scheduler.task_failed(recv_dict['wid'], tid, val['time_start'], val['time_fin'],val['errcode'])
                            else:
                                master_log.error('Task msg incomplete, key=%s'%val.keys())
                    elif recv_dict.has_key('health'):
                        # TODO handle node health
                        pass
                    elif recv_dict.has_key('wstatus'):
                        wentry = self.worker_registry.get_entry(recv_dict['wid'])
                        wentry.setStatus(recv_dict['wstatus'])
                    else:
                        for k,v in recv_dict:
                            if not k in [str(MPI_Wrapper.Tags.APP_INI),
                                         str(MPI_Wrapper.Tags.TASK_ADD),
                                         str(MPI_Wrapper.Tags.APP_FIN),
                                         str(MPI_Wrapper.Tags.LOGOUT)]:
                                continue
                        # handle acquires
                            if int(k) == MPI_Wrapper.Tags.APP_INI:
                                if v['recode'] == status.SUCCESS:
                                    self.task_scheduler.worker_initialized(recv_dict['wid'])
                                    master_log.info('worker %d initialize successfully'%recv_dict['wid'])
                                else:
                                    # initial worker failed
                                    # TODO: Optional, add other operation
                                    master_log.error('worker %d initialize error, errmsg=%s'%(recv_dict['wid'], v['errmsg']))
                                    # check re-initial times in policy
                                    if self.worker_registry.worker_reinit(v['wid']):
                                        send_dict = {'wid': v['wid'], 'appid': self.task_scheduler.appid,
                                                     'init': self.task_scheduler.init_worker(),
                                                     'tag': MPI_Wrapper.Tags.MPI_REGISTY_ACK, 'uuid': recv_dict['uuid']}
                                        self.command_q.put(send_dict)
                                    else:
                                        #  terminate worker
                                        send_dict = {MPI_Wrapper.Tags.WORKER_STOP:""}
                                        self.command_q.put(send_dict)
                            # worker ask for new tasks
                            elif int(k) == MPI_Wrapper.Tags.TASK_ADD:
                                wentry = self.worker_registry.get_entry(recv_dict['wid'])
                                if v != wentry.max_capacity-wentry.assigned:
                                    wentry.alive_lock.acqurie()
                                    wentry.assigned = wentry.max_capacity-v
                                    wentry.alive_lock.release()
                                task_list = self.task_scheduler.assignTask(self.worker_registry.get_entry(recv_dict['wid']))
                                if not task_list:
                                    tmp_dict = self.task_scheduler.fin_worker()
                                    self.command_q.put({MPI_Wrapper.Tags.APP_FIN:tmp_dict,'uuid':recv_dict['uuid']})
                                task_dict = {}
                                for tid in task_list:
                                    task_dict[tid] = {'boot': self.appmgr.get_task(tid).boot,
                                                      'args': self.appmgr.get_task(tid).args,
                                                      'data': self.appmgr.get_task(tid).data,
                                                      'resdir': self.appmgr.get_task(tid).res_dir}
                                    master_log.info('Assign task %d to worker %d'%(tid,recv_dict['wid']))

                                self.command_q.put({MPI_Wrapper.Tags.TASK_ADD: task_dict, 'uuid':recv_dict['uuid']})
                            # worker finalized
                            elif int(k) == MPI_Wrapper.Tags.APP_FIN:
                                if v['recode'] == status.SUCCESS:
                                    self.task_scheduler.worker_finalized(recv_dict['wid'])
                                    master_log.info('worker %d finalized')
                                    # TODO if has new app, return Tags.new_app, or return Tags.Logout
                                else:
                                    master_log.error('worker %d finalize error, errmsg=%s' % (recv_dict['wid'], v['errmsg']))
                                    if self.worker_registry.worker_refin(recv_dict['wid']):
                                        tmp_dict = self.task_scheduler.fin_worker()
                                        self.command_q.put({MPI_Wrapper.Tags.APP_FIN: tmp_dict, 'uuid': recv_dict['uuid']})
                                    else:
                                        send_dict = {MPI_Wrapper.Tags.WORKER_STOP: ""}
                                        self.command_q.put(send_dict)

            if not self.task_scheduler.has_more_work() and not self.task_scheduler.has_scheduled_work():
                self.appmgr.finalize_app()

            send_dict = {}
            while not self.command_q.empty():
                tmp_dict = dict(send_dict, **self.command_q.get())
                if "tag" in tmp_dict.keys():
                    send_dict = {tmp_dict['tag']: tmp_dict}
                else:
                    send_dict = {MPI_Wrapper.Tags.MPI_PING: tmp_dict}
            if len(send_dict) != 0:
                send_str = json.dumps(send_dict)
                master_log.debug('[Master] Send msg = %s'%send_str)
                self.server.send_string(send_str, len(send_str), send_dict['uuid'], send_dict.keys()[0])


    def check_msg_integrity(self, tag, msg):
        if tag == 'Task':
            return set(['task_stat', 'time_start', 'time_fin', 'errcode']).issubset(set(msg.keys()))
        elif tag == 'firstPing':
            return set(['uuid']).issubset(set(msg.keys()))
            #return set(['uuid', 'capacity']).issubset(set(msg.keys()))
        elif tag == 'health':
            return set(['CpuUsage','MemoUsage']).issubset(set(msg.keys()))





