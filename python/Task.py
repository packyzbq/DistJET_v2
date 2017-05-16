import time
import logging

class TaskStatus:
    """
    task status enumeration
    """
    NEW             = 0
    INITIALIZED     = 1
    PROCESSING      = 2
    COMPLETED       = 3
    FAILED          = 4
    LOST            = 5
    HALT            = 6 # have scheduled , to be performed

class TaskDetail:
    """
    Details about tasks' status for a single execution attempt
    """

    def __init__(self):
        self.assigned_wid = -1
        self.time_start = 0
        self.time_exec = 0
        self.time_end = 0
        self.time_scheduled = 0

        self.error = None

    def assign(self, wid):
        if wid <= 0:
            return False
        else:
            self.assigned_wid = wid
            self.time_scheduled = time.time()
            return True

    def fail(self, time_start, time_finish=time.time(), error_code=0):
        self.time_start = time_start
        self.time_end = time_finish
        self.error = error_code

    def complet(self, time_start, time_end):
        self.fail(time_start, time_end)


class Task:
    """
    The object split from application. Include a tracer to record the history
    """
    def __init__(self, tid):
        self.tid = tid
        self.status = TaskStatus.NEW
        self.history = [TaskDetail()]

        self.boot = []
        self.data = {}
        self.args = {}

        self.res_dir = None

    def initial(self, work_script=None, args = {}, data = {}, res_dir="./"):
        self.boot = work_script
        self.res_dir = res_dir
        self.data = data
        self.args = args
        self.status = TaskStatus.INITIALIZED

    def status(self):
        return self.status

    def fail(self, time_start, time_end=time.time(), error = 0):
        self.status = TaskStatus.FAILED
        self.history[-1].fail(time_start, time_end, error)

    def complete(self, time_start, time_end):
        self.status = TaskStatus.COMPLETED
        self.history[-1].complet(time_start,time_end)

    def assign(self, wid):
        if not self.status is TaskStatus.INITIALIZED:
            self.history.append(TaskDetail())
        self.history[-1].assign(wid)
        self.status = TaskStatus.HALT

    def getdata(self):
        return self.data



