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
        if not os.path.exists(os.path.abspath(config_path)):
            self.log.warning('[ValApp] Can not find config file = %s, use default'%config_path)
            config_path = os.environ['DistJETPATH']+'/Application/Validation/config.ini'
        self.cfg = AppConf(config_path,'ValApp')
        if self.cfg.get('topDir'):
            self.topdir=self.cfg.get('topDir')
            self.status['topdir']=True
        '''
        with open(config_path,'r') as f:
            for line in f.readlines():
                key = line.split('=')[0].split()[0]
                val = line.split('=')[1].split()
                self.config[key] = val
        '''
    def getcfgattr(self,item):
        if self.conf.get(item):
            return self.conf.get(item)
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
        topworkdir = self.cfg.get('workdir')
        anaflow = self.cfg.get('anawork')
        if type(anaflow) != types.ListType:
            anaflow = anaflow.split()
        self.log.debug('anaflow = %s'%anaflow)
        data_index = 0
        self.log.debug('cfg = %s'%self.cfg.get('&'))
        if topworkdir and topworkdir != 'None':
            self.log.debug('%s'%os.listdir(topworkdir))
            for subworkdir in os.listdir(topworkdir):
                subworkdir = os.path.join(topworkdir,subworkdir)
                if os.path.isdir(os.path.abspath(subworkdir)):
                    for tagdir in os.listdir(subworkdir):
                        tagdir = os.path.join(subworkdir,tagdir)
                        if os.path.isdir(tagdir):
                            for anastep in anaflow:
                                self.data[data_index] = {"workdir":os.path.abspath(tagdir),'step':anastep,
                                                         'anaflag':self.cfg.get('anaflag'),'cmpflag':self.cfg.get('cmpflag'),
                                                         'refpath':self.cfg.get('refpath'),'anascript':self.cfg.get(anastep)}
                                data_index+=1
                        elif os.path.basename(tagdir) in anaflow:
                            self.data[data_index] = {"workdir": os.path.abspath(subworkdir), 'step': os.path.basename(tagdir),
                                                     'anaflag': self.cfg.get('anaflag'), 'cmpflag': self.cfg.get('cmpflag'),
                                                     'refpath':self.cfg.get('refpath'),'anascript':self.cfg.get(os.path.basename(tagdir))}
                            data_index+=1
                else:
                    self.log.warning('subworkdir %s is not a valid dir'%os.path.abspath(subworkdir))
        self.log.debug('self.data = %s'%self.data)
        return self.data

    def merge(self,tasklist):
        pass



