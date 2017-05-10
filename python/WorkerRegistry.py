wRegistery_log = None

import time
import threading
import traceback

class WorkerStatus:
    NEW = -1
    INITILAZED = 0
    IDLE = 1
    RUNNING = 2
    ERROR = 3
    LOST = 4
    FINALIZED = 5

class WorkerEntry:
    """
    contain worker information and task queue
    """
    def __init__(self, wid, w_uuid, max_capacity):
        self.wid = wid
        self.w_uuid = w_uuid
        self.registration_time =time.time()
        self.last_contact_time = self.registration_time
        self.idle_time = 0

        self.max_capacity = max_capacity
        self.assigned = 0

        self.status = WorkerStatus.NEW

    def capacity(self):
        return self.max_capacity-self.assigned

    def isLost(self):
        # FIXME: add LOST_WORKER_TIMEOUT into config
        return time.time()-self.last_contact_time > LOST_WORKER_TIMEOUT

    def getStatus(self):
        return self.status
    def isIdle_timeout(self):
        # FIXME: add IDLE_WORKER_TIMEOUT into config
        return self.idle_time and IDLE_WORKER_TIMEOUT and time.time()-self.idle_time > IDLE_WORKER_TIMEOUT

class WorkerRegistery:
    