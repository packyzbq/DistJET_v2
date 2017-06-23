

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

    def do_work(self,boot=None,data={},args={},resdir=None,extra={}):
        """
        The worker process doing worker
        :param kwargs: boot=boot, args=args, data=data, resdir=resdir, logfile=logFile
        :return: return code-> succ/fail/not return...
        """
        raise NotImplementedError
