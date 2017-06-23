from IAPPWorker import IAPPWorker
from python.WorkerAgent import status
class TestWorker(IAPPWorker):
    def initialize(self,**kwargs):
        self.log.info("TestWorker initialized")
        return True

    def finalize(self, **kwargs):
        self.log.info("TestWorker finalized")
        return True

    def do_work(self,boot=None,data={},args={},resdir=None,extra={},log=None):
        self.log.info("TestWorker doing work...,")
        return status.SUCCESS