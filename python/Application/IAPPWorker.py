import python.Util.logger as logger

class IAPPWorker:
    def __init__(self):
        self.log = logger.getLogger(self.__class__.__name__)

    def initialize(self,**kwargs):
        raise NotImplementedError

    def finalize(self, logfile, **kwargs):
        raise NotImplementedError

    def do_work(self,**kwargs):
        raise NotImplementedError


class TestWorker(IAPPWorker):

    def initialize(self):
        pass