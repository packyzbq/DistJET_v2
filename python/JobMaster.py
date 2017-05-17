import json
import time,datetime
import sys
import logging
import Queue

control_log = logging.getLogger('Control_Log')
master_log = logging.getLogger('Master')

import IR_Buffer_Module as IM
import IScheduler
import WorkerRegistry
WorkerRegistry.wRegistery_log = master_log
import MPI_Wrapper
MPI_Wrapper.MPI_log = master_log

from BaseThread import BaseThread
from IAppManager import SimpleAppManager
from Task import TaskStatus
from Conf import Policy,GlobalCfg
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

            time.sleep(Policy.getAttr('CONTROL_DELAY'))

    def activateProcessing(self):
        self.processing = True

class IJobMaster:
    """
    interface used by Scheduler to control task scheduling
    """
    pass

class JobMaster(IJobMaster):
    def __init__(self, cfg_path = None, applications=[]):
        if not cfg_path:
            #TODO use default path
            pass
        Policy.loadPolicy(cfg_path)
        GlobalCfg.loadCfg(cfg_path)
        self.svc_name = GlobalCfg.getAttr('svc_name')
        if not self.svc_name:
            self.svc_name = 'Default'
        # worker list
        self.worker_registry = WorkerRegistry.WorkerRegistry()
        # scheduler
        self.task_scheduler = None
        self.appmgr = None

        self.recv_buffer = IM.IRecv_buffer()

        self.control_thread = ControlThread(self)

        self.applications = applications

        self.command_q = Queue.Queue()

        self.__tid = 1
        self.__wid = 1

        self.server = MPI_Wrapper.Server(self.recv_buffer,self.svc_name)
        self.server.initialize()

        self.__stop = False

    def stop(self):
        self.task_scheduler.join()
        master_log.info('[Master] TaskScheduler has joined')
        self.control_thread.stop()
        self.control_thread.join()
        master_log.info('[Master] Control Thread has joined')
        self.server.stop()
        master_log.info('[Master] Server stop')
        self.__stop = True

    def register_worker(self, w_uuid, capacity = 1):
        worker = self.worker_registry.add_worker(w_uuid, capacity)
        if not worker:
            master_log.warning('[Master] The uuid=%s of worker has already registered', w_uuid)
        else:
            send_str = MSG_wrapper(wid=worker.wid,appid=self.task_scheduler.appid, init={self.task_scheduler.init_worker})
            self.server.send_string(send_str, len(send_str), w_uuid, MPI_Wrapper.Tags.MPI_REGISTY_ACK)



    def remove_worker(self,wid):
        self.worker_registry.remove_worker(wid)
        self.task_scheduler.worker_removed(wid, datetime.datetime.now())

    def anaylize_health(self, info):
        pass

    def startProcessing(self):
        self.appmgr = SimpleAppManager(apps=self.applications)
        #self.task_scheduler = IScheduler.SimpleScheduler(self.appmgr,self.worker_registry)
        if self.appmgr.get_current_app().scheduler:
            self.task_scheduler = self.appmgr.get_current_app().scheduler(self.appmgr, self.worker_registry)
        else:
            self.task_scheduler = IScheduler.SimpleScheduler(self.appmgr,self.worker_registry)
        while not self.__stop:
            if not self.recv_buffer.empty():
                msg = self.recv_buffer.get()
                if msg.tag == -1:
                    continue
                recv_dict = json.loads(msg.sbuf[0:msg.size])
                #TODO handle node health
                if recv_dict.has_key('flag'):
                    if recv_dict['flag'] == 'firstPing':
                        # register worker
                        # check dict's integrity
                        if self.check_msg_integrity('firstPing', recv_dict):
                            self.register_worker(recv_dict['uuid'], recv_dict['capacity'])
                        else:
                            master_log.error('firstPing msg incomplete, key=%s'%recv_dict.keys())
                        self.command_q.put({MPI_Wrapper.Tags.APP_INI: self.appmgr.get_app_init(self.appmgr.current_app_id)})
                    elif recv_dict['flag'] == 'lastPing':
                        # last ping from worker, sync completed task, report node's health, logout and disconnect worker
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
                    for tid, val in recv_dict['Task']:
                        # handle complete task
                        if self.check_msg_integrity('Task',val):
                            if val['task_stat'] == TaskStatus.COMPLETED:
                                self.task_scheduler.task_completed(recv_dict['wid'], tid, val['time_start'], val['time_fin'])
                            elif val['task_stat'] == TaskStatus.FAILED:
                                self.task_scheduler.task_failed(recv_dict['wid'], tid, val['time_start'], val['time_fin'],val['errcode'])
                        else:
                            master_log.error('Task msg incomplete, key=%s'%val.keys())
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
                        # worker ask for new tasks
                        elif int(k) == MPI_Wrapper.Tags.TASK_ADD:
                            wentry = self.worker_registry.get_entry(recv_dict['wid'])
                            if v != wentry.max_capacity-wentry.assigned:
                                wentry.alive_lock.acqurie()
                                wentry.assigned = wentry.max_capacity-v
                                wentry.alive_lock.release()
                            task_list = self.task_scheduler.assignTask(self.worker_registry.get_entry(recv_dict['wid']))
                            if not task_list:
                                self.command_q.put({MPI_Wrapper.Tags.APP_FIN:{}})
                            task_dict = {}
                            for tid in task_list:
                                task_dict[tid] = {'boot': self.appmgr.get_task(tid).boot,
                                                  'args': self.appmgr.get_task(tid).args,
                                                  'data': self.appmgr.get_task(tid).data,
                                                  'resdir': self.appmgr.get_task(tid).res_dir}
                                master_log.info('Assign task %d to worker %d'%(tid,recv_dict['wid']))

                            self.command_q.put({MPI_Wrapper.Tags.TASK_ADD: task_dict})
                        # worker finalized
                        elif int(k) == MPI_Wrapper.Tags.APP_FIN:
                            if v['recode'] == status.SUCCESS:
                                self.task_scheduler.worker_finalized(recv_dict['wid'])
                                master_log.info('worker %d finalized')
                            else:
                                # TODO: Optional, add other operation
                                master_log.error('worker %d finalize error, errmsg=%s'%(recv_dict['wid'],v['errmsg']))
                        # worker ready to logout
                        elif int(k) == MPI_Wrapper.Tags.LOGOUT:
                            # TODO: Optional, add other operation
                            self.command_q.put({MPI_Wrapper.Tags.LOGOUT_ACK:''})
            send_dict = {}
            while not self.command_q.empty():
                send_dict = dict(send_dict, **self.command_q.get())
            send_str = json.dumps(send_dict)
            self.server.send_string(send_str, len(send_str), self.worker_registry.get_entry(recv_dict['wid']).w_uuid, MPI_Wrapper.Tags.MPI_PING)




    def check_msg_integrity(self, tag, msg):
        if tag == 'Task':
            return {'task_stat', 'time_start', 'time_fin', 'errcode'}.issubset(set(msg.keys()))
        elif tag == 'firstPing':
            return {'uuid', 'capacity'}.issubset(set(msg.keys()))
        elif tag == 'health':
            return {'CpuUsage','MemoUsage'}.issubset(set(msg.keys()))





