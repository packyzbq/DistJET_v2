import types
import threading
import os
import ConfigParser

#os.environ['DistJETPATH'] = "/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2"
GlobalLock = threading.RLock()
_inipath = None

def set_inipath(inipath):
    try:
        GlobalLock.acquire()
        global _inipath
        _inipath = inipath
    finally:
        GlobalLock.release()

class Config(object):
    __global_config = {
        'Log_Level': 'debug',
        'health_detect_scripts': None,
        'topDir': os.environ['DistJETPATH'],
        'Rundir': None
    }

    __policy = {
        'LOST_WORKER_TIMEOUT': 60,
        'IDLE_WORKER_TIMEOUT': 100,
        'CONTROL_DELAY': 1,
        'ATTEMPT_TIME': 2
    }

    __loaded = False

    def __new__(cls, *args, **kwargs):
        global _inipath
        if not cls.__loaded:
            if _inipath :
                if os.path.exists(_inipath):
                    pass
                elif os.path.exists(os.environ['DistJETPATH']+'/'+_inipath):
                    set_inipath(os.environ['DistJETPATH']+'/'+_inipath)
                else:
                    return object.__new__(cls)
                try:
                    GlobalLock.acquire()
                    cf = ConfigParser.ConfigParser()
                    cf.read(_inipath)
                    if cf.has_section('GlobalCfg'):
                        for key in cf.options('GlobalCfg'):
                            cls.__global_config[key] = cf.get('GlobalCfg', key)
                    if cf.has_section('Policy'):
                        for key in cf.options('Policy'):
                            cls.__policy[key] = cf.getint('Policy', key)
                    cls.__loaded = True
                finally:
                    GlobalLock.release()
        return object.__new__(cls)

    @classmethod
    def getCFGattr(cls,key):
        try:
            GlobalLock.acquire()
            if key in cls.__global_config.keys():
                return cls.__global_config[key]
            else:
                return None
        finally:
            GlobalLock.release()
    @classmethod
    def getPolicyattr(cls,key):
        try:
            GlobalLock.acquire()
            if key in cls.__policy.keys():
                return cls.__policy[key]
            else:
                return None
        finally:
            GlobalLock.release()

    @classmethod
    def setCfg(cls, key, val):
        try:
            GlobalLock.acquire()
            cls.__global_config[key] = val
        finally:
            GlobalLock.release()

    @classmethod
    def setPolicy(cls, key, val):
        try:
            GlobalLock.acquire()
            cls.__policy[key] = val
        finally:
            GlobalLock.release()
	
    @classmethod
    def isload(cls):
        return cls.__loaded


class AppConf:
    __cfg = {
        'appid':None,
        'appName': None,
        'workDir': None,
        'topDir': Config.getCFGattr('topDir')
    }


    def __init__(self, ini_path=None):
        self.config = dict([(k, v) for (k, v) in AppConf.__cfg.items()])
        self.lock = threading.RLock()
        if ini_path:
            if os.path.exists(ini_path):
                pass
            elif os.path.exists(os.environ['DistJETPATH']+'/'+ini_path):
                ini_path = os.environ['DistJETPATH']+'/'+ini_path
            else:
                return
            try:
                self.lock.acquire()
                cf = ConfigParser.ConfigParser()
                cf.read(ini_path)
                if cf.has_section('AppConfig'):
                    for key in cf.options('AppConfig'):
                        self.config[key] = cf.get('AppConfig',key)
            finally:
                self.lock.release()

    def __getattr__(self,item):
        assert type(item) == types.StringType, "ERROR: attribute must be of String type!"
        if self.config.has_key(item):
            return self.config[item]
        else:
            return None

    def __setattr__(self,key, value):
        assert type(key) == types.StringType, "ERROR: attribute must be of String type!"
        self.config[key] = value

if __name__ == '__main__':
    set_inipath('/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/config/default.cfg')
    config = Config()
    if config.__class__.isload():
        print 'config file loaded'
    print config.__class__.getCFGattr('svc_name')
