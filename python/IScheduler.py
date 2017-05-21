import json
import Queue
import time
import logging
import WorkerRegistry
import Conf
import Task
from MPI_Wrapper import Tags
from BaseThread import BaseThread

scheduler_log = logging.getLogger('TaskScheduler')

def MSG_Wrapper(**kwd):
    return json.dumps(kwd)

class IScheduler:
    def __init__(self, appmgr, worker_registry=None):
        #self.master = master
        self.appid = None
        self.worker_registry = worker_registry
        self.appmgr = appmgr
        self.task_todo_queue = Queue.Queue()
        for tid, task in self.appmgr.get_app_task_list():
            self.task_todo_queue.put({tid,task})
        self.scheduled_task_list = {}       # wid: tid_list
        self.completed_queue = Queue.Queue()

    def initialize(self):
        """
        Initialize the TaskScheduler passing the job input parameters as specified by the user when starting the run.
        :return:
        """
        pass

    def run(self):
        """
        :return:
        """
        pass

    def finalize(self):
        """
        The operation when Scheduler exit
        :return:
        """
        pass

    def assignTask(self, w_entry):
        """
        The master call this method when a Worker ask for tasks
        :param w_entry:
        :return: a list of assigned task id
        """
        raise NotImplementedError

    def setWorkerRegistry(self, worker_registry):
        """
        :param worker_registry:
        :return:
        """
        self.worker_registry = worker_registry

    def has_more_work(self):
        """
        Return ture if current app has more work( when the number of works of app is larger than sum of workers' capacities)
        :return: bool
        """
        return not self.task_todo_queue.empty()

    def has_scheduled_work(self):
        return len(self.scheduled_task_list)!=0

    def task_failed(self, wid, tid, time_start, time_finish, error):
        """
        called when tasks completed with failure
        :param wid: worker id
        :param tid: task id
        :param time_start:  the start time of the task, used for recoding
        :param time_finish: the end time of the task, used for recoding
        :param error: error code of the task
        :return:
        """
        raise NotImplementedError

    def task_completed(self, wid, tid, time_start, time_finish):
        """
        this method is called when task completed ok.
        :param wid:
        :param tid:
        :param time_start:
        :param time_finish:
        :return:
        """
        raise NotImplementedError

    def init_worker(self):
        app = self.appmgr.get_current_app()
        task_dict = {}
        task_dict['boot'] = app.app_init_boot
        task_dict = dict(task_dict, **app.app_init_extra)
        task_dict['resdir'] = app.res_dir
        return task_dict

    def worker_initialized(self, wid):
        """
        called by Master when a worker agent successfully initialized the worker, (maybe check the init_output)
        when the method returns, the worker can be marked as ready
        :param wid:
        :return:
        """
        raise NotImplementedError

    def worker_finalized(self, wid):
        """

        :param wid:
        :return:
        """
        raise  NotImplementedError

    def worker_added(self, wid):
        """
        This method is called by RunMaster when the new worker agent is added. Application specific initialization data
        may be assigned to w_entry.init_input at this point.
        :param wid:
        :return:
        """
        raise NotImplementedError

    def worker_removed(self, wid, time_point):
        """
        This method is called when the worker has been removed (either lost or terminated due to some reason).
        :param wid:
        :return:
        """
        raise

class SimpleScheduler(IScheduler):

    def assignTask(self, w_entry):
        if not w_entry.alive:
            return None
        room = w_entry.capacity()
        task_list=[]
        self.scheduled_task_list[w_entry.wid] = []
        for i in range(0,room):
            tmptask = self.task_todo_queue.get()
            tmptask.assign(w_entry.wid)
            task_list.append(tmptask.tid)
            self.scheduled_task_list[w_entry.wid].append(tmptask.tid)
        return task_list

    def task_failed(self, wid, tid, time_start, time_finish, error):
        tmptask = self.appmgr.get_task(tid)
        tmptask.fail(time_start,time_finish,error)
        self.task_todo_queue.put(tmptask)
        if tid in self.scheduled_task_list[wid]:
            self.scheduled_task_list[wid].remove(tid)

    def task_completed(self, wid, tid, time_start, time_finish):
        tmptask = self.appmgr.get_task(tid)
        tmptask.complete(time_start,time_finish)
        if tid in self.scheduled_task_list[wid]:
            self.scheduled_task_list[wid].remove(tid)

    def worker_initialized(self, wid):
        entry = self.worker_registry.get_entry(wid)
        try:
            entry.alive_lock.acquire()
            entry.status = WorkerRegistry.WorkerStatus.INITILAZED
        finally:
            entry.alive_lock.release()

    def worker_added(self, wid):
        # TODO
        pass

    def worker_removed(self, wid, time_point):
        for tid in self.scheduled_task_list[wid]:
            self.appmgr.get_task(tid).withdraw(time_point)
            self.task_todo_queue.put(self.appmgr.get_task(tid))
            self.scheduled_task_list[wid].remove(tid)

    def worker_finalized(self, wid):
        w = self.worker_registry.get_entry(wid)
        if w.alive:
            try:
                w.alive_lock.acquire()
                w.alive = False
                w.status = WorkerRegistry.WorkerStatus.FINALIZED
            finally:
                w.alive_lock.release()





