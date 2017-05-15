import json
import Queue
import time
import logging
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
        self.scheduled_task_list = {}
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
        raise NotImplementedError

    def finalize(self):
        """
        The operation when Scheduler exit
        :return:
        """
        raise NotImplementedError

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

    def worker_removed(self, wid):
        """
        This method is called when the worker has been removed (either lost or terminated due to some reason).
        :param wid:
        :return:
        """
        raise

class SimpleScheduler(IScheduler):
    pass