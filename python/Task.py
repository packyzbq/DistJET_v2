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
        self.info = None

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
        self.info = 'Fail'

    def complete(self, time_start, time_end):
        self.time_start = time_start
        self.time_end = time_end
        self.info = 'Complete'

    def withdraw(self, time_term):
        self.time_end = time_term
        self.info = 'Cancel'




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

    def initial(self, work_script=None, args=None, data = None, res_dir="./"):
        """
        :param work_script: the script path
        :param args: {}
        :param data: {}
        :param res_dir:
        :return:
        """
        if args is None:
            args = {}
        self.boot = work_script
        self.res_dir = res_dir
        self.data = data
        self.args = args
        self.status = TaskStatus.INITIALIZED

    def toDict(self):
        tmpdict = {}
        tmpdict['boot'] = self.boot
        tmpdict['data'] = self.data
        tmpdict['args'] = self.args
        tmpdict['resdir'] = self.res_dir
        return tmpdict       

    def status(self):
        return self.status

    def fail(self, time_start, time_end=time.time(), error = 0):
        self.status = TaskStatus.FAILED
        self.history[-1].fail(time_start, time_end, error)

    def complete(self, time_start, time_end):
        self.status = TaskStatus.COMPLETED
        self.history[-1].complete(time_start,time_end)

    def assign(self, wid):
        if not self.status is TaskStatus.INITIALIZED:
            self.history.append(TaskDetail())
        self.history[-1].assign(wid)
        self.status = TaskStatus.HALT

    def withdraw(self, time_term):
        self.status = TaskStatus.INITIALIZED
        self.history[-1].withdraw(time_term)



    def getdata(self):
        return self.data



