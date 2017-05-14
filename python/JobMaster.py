import json
import time
import sys
import logging

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
from Conf import Policy

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
                        if w.alive and w.lost():
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
                            if w.idle_timeout():
                                # idle timeout, worker will be removed
                                control_log.warning('worker %d idle too long and will be removed', wid)
                                self.master.remove_worker(wid)
                    finally:
                        w.alive_lock.release()
            finally:
                pass

            time.sleep(self.master.policy.CONTROL_DELAY)

    def activateProcessing(self):
        self.processing = True

class IJobMaster:
    """
    interface used by Scheduler to control task scheduling
    """
    pass

class JobMaster(IJobMaster):
    def __init__(self, policy, applications=[]):
        self.svc_name = policy.svc_name
        # worker list
        self.worker_registry = WorkerRegistry.WorkerRegistry()
        # scheduler
        self.task_scheduler = None

        self.recv_buffer = IM.IRecv_buffer()

        self.control_thread = ControlThread(self)

        self.applications = applications

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

    def startProcessing(self):
        simple_appmgr = SimpleAppManager(apps=self.applications)
        self.task_scheduler = IScheduler.SimpleScheduler(simple_appmgr,)
        while not self.__stop:
            if not self.recv_buffer.empty():
                msg = self.recv_buffer.get()
                if msg.tag == -1:
                    continue
                recv_dict = json.loads(msg.sbuf[0:msg.size])
                #TODO add ping time
                for k,v in recv_dict:




