from python.WorkerAgent import status

class IAPPWorker:
    def __init__(self, log):
        self.log = log

    def initialize(self,**kwargs):
        """
        :param kwargs: boot=boot, args=args,data=data,resdir=resdir
        :return: False/True
        """
        raise NotImplementedError

    def finalize(self, **kwargs):
        """
        :param kwargs:  boot=boot, args=args,data=data,resdir=resdir
        :return: False/True
        """
        raise NotImplementedError

    def do_work(self,**kwargs):
        """
        The worker process doing worker
        :param kwargs: boot=boot, args=args, data=data, resdir=resdir, logfile=logFile
        :return: return code-> succ/fail/not return...
        """
        raise NotImplementedError


class TestWorker(IAPPWorker):
    def initialize(self,**kwargs):
        self.log.info("TestWorker initialized")
        return True

    def finalize(self, **kwargs):
        self.log.info("TestWorker finalized")
        return True

    def do_work(self,**kwargs):
        self.log.info("TestWorker doing work..., args=%s, data=%s"%(kwargs['args'],kwargs['data']))
        return status.SUCCESS