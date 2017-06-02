import os
import subprocess
import types

from python import IScheduler
from IAPPWorker import IAPPWorker


class IApplication:
    def __init__(self):
        self.id = None
        self.app_boot=[]
        self.res_dir = ""   # the directory of result
        self.args = {}      # the args for app_boot
        self.data = {}      # k-v

        self.app_init_boot = []     # the prog for app init
        self.app_init_extra = {}    # the args/data for running init prog

        self.app_fin_boot = []
        self.app_fin_extra = {}

        self.scheduler = None
        self.specifiedWorker = None
        self.log = None
        self.rootdir = None
    def set_id(self,id):
        self.id = id

    def set_scheduler(self, scheduler):
        if not callable(scheduler) or not issubclass(scheduler,IScheduler.IScheduler):
            # TODO unrecognized scheduler
            print('Scheduler %s can not be recognized'%scheduler)
            return
        else:
            self.scheduler = scheduler

    def set_worker(self, worker):
        if not callable(worker) or not issubclass(worker, IAPPWorker):
            print('Costumed Worker %s can not be recognized, use default worker'%worker)
            return
        else:
            self.specifiedWorker = worker

    def get_scheduler(self):
        return self.scheduler

    def set_init_boot(self,init_boot):
        if type(init_boot) is types.ListType:
            self.app_init_boot.extend(init_boot)
        else:
            self.app_init_boot.append(init_boot)
    def set_init_extra(self, init_extra):
        """
        :param init_extra: dict
        :return:
        """
        if not type(init_extra) is types.DictionaryType:
            return
        self.app_init_extra=dict(self.app_init_extra,**init_extra)
    def set_fin_boot(self, fin_boot):
        if type(fin_boot) is types.ListType:
            self.app_fin_boot.extend(fin_boot)
        else:
            self.app_fin_boot.append(fin_boot)
    def set_fin_extra(self, fin_extra):
        """
        :param fin_extra: dict
        :return:
        """
        if not type(fin_extra) is types.DictionaryType:
            return
        self.app_fin_extra=dict(self.app_fin_extra,**fin_extra)
    def set_boot(self, boot_list):
        if type(boot_list) is types.ListType:
            self.app_boot.extend(boot_list)
        else:
            self.app_boot.append(boot_list)
    def set_resdir(self, res_dir):
        self.res_dir = os.path.abspath(res_dir)
        if not os.path.exists(self.res_dir):
            os.mkdir(self.res_dir)
    def set_rootdir(self, rootdir):
        if os.path.exists(rootdir):
            self.rootdir = os.path.abspath(rootdir)

    def split(self):
        """
        this method needs to be overwrite by user to split data into key-value pattern
        :return: k-v data
        """
        raise NotImplementedError

    def merge(self, tasklist):
        """
        this method needs to be overwrite by user to merge the result data
        :param data: dict type
        :return:
        """
        raise NotImplementedError
