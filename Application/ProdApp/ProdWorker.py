from python.Application.IAPPWorker import IAPPWorker
from python.Process import Process

class ProdWorker(IAPPWorker):
    def initialize(self,**kwargs):
        pass

    def finalize(self, **kwargs):
        pass

    def do_work(self,boot=None,data={},args={},resdir=None,log=None):
        self.log.debug('[ProdWorker] start run task....')
