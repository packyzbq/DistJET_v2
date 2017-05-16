import os
import subprocess
import types

class IApplication:
    def __init__(self):
        self.app_boot=[]
        self.res_dir = ""   # the directory of result
        self.args = {}      # the args for app_boot
        self.data = {}      # k-v

        self.app_init_boot = []     # the prog for app init
        self.app_init_extra = {}    # the args/data for running init prog

        self.app_fin_boot = []
        self.app_fin_extra = {}

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
        self.res_dir = res_dir

    def split(self,data, **kwd):
        """
        this method needs to be overwrite by user to split data into key-value pattern
        :return: k-v data
        """
        raise NotImplementedError

    def merge(self, data, **kwd):
        """
        this method needs to be overwrite by user to merge the result data
        :param data: dict type
        :return:
        """
        raise NotImplementedError