from python.Application.IApplication import IApplication
from python.Util.Conf import AppConf
import os
import types

class ValApp(IApplication):
    def __init__(self, rootdir, name, config_path):
        IApplication.__init__(self, rootdir, name)
        self.topdir=None
        self.anawork=[]
        self.refdir = None
        self.status['topdir']=False
        self.ana_flag = True
        self.cmp_flag = True
        self.config = {}
        if not os.path.exists(os.path.abspath(config_path)):
            self.log.warning('[ValApp] Can not find config file = %s, use default'%config_path)
            config_path = os.environ['DistJETPATH']+'/Application/Validation/config.ini'
        self.cfg = AppConf(config_path)
        '''
        with open(config_path,'r') as f:
            for line in f.readlines():
                key = line.split('=')[0].split()[0]
                val = line.split('=')[1].split()
                self.config[key] = val
        '''

    def __getattr__(self, item):
        if self.config.has_key(item):
            return self.config[item]
        else:
            return None

    def set_topdir(self, topdir):
        if os.path.exists(os.path.abspath(topdir)):
            self.topdir = topdir
            self.status['topdir'] = True
        else:
            self.log.error('@set topdir error, no such dir=%s'%os.path.abspath(topdir))

    def set_anawork(self,anawork):
        if anawork and type(anawork) == types.ListType:
            self.anawork.extend(anawork)

    def split(self):
        # 根据类型， 目录拆分，保有 ref的文件目录路径

