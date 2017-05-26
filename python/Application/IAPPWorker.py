

class IAPPWorker:
    def __init__(self):
        pass

    def initialize(self,**kwargs):
        raise NotImplementedError

    def finalize(self, logfile, **kwargs):
        raise NotImplementedError

    def do_work(self,**kwargs):
        raise NotImplementedError


class TestWorker(IAPPWorker):
    