import types
import threading
import os
import ConfigParser

PolicyLock = threading.RLock()
CfgLock = threading.RLock()

class AppConf:
    defaults = {
        'appid':None,
        'appName': None,
        'workDir': None,
        'topDir': GlobalCfg.getAttr('topDir')
    }

    def __init__(self, appid, appName):
        self.cfg = dict([(k, v) for (k, v) in AppConf.defaults.items()])

    def __getattr__(self, item):
        assert type(item) == types.StringType, "ERROR: attribute must be of String type!"
        if self.cfg.has_key(item):
            return self.cfg[item]
        else:
            return None
    def __setattr__(self, key, value):
        assert type(key) == types.StringType, "ERROR: attribute must be of String type!"
        self.cfg[key] = value


class GlobalCfg:
    defaults={
        'health_detect_scripts': None,
        'topDir': None
    }

    __config = None
    def __init__(self,cfg_path=None):
        GlobalCfg.__config = dict([(k,v) for (k,v) in GlobalCfg.defaults.items()])
        # FIXME: Need abs path
        if cfg_path and os.path.exists(cfg_path):
            cf = ConfigParser.ConfigParser()
            cf.read(cfg_path)
            if cf.has_section('Cfg'):
                for key in cf.options('Cfg'):
                    GlobalCfg.__config[key] = cf.getint('Cfg',key)

    @staticmethod
    def getCfg(cfg_path=None):
        if GlobalCfg.__config is None:
            try:
                CfgLock.acquire()
                if GlobalCfg.__config is None:
                    GlobalCfg(cfg_path)
            finally:
                CfgLock.release()
        return GlobalCfg.__config

    @staticmethod
    def setAttr(name, value):
        assert type(name) == types.StringType, "ERROR: attribute must be of String type!"
        try:
            CfgLock.acquire()
            GlobalCfg.__config[name] = value
        finally:
            CfgLock.release()

    @staticmethod
    def getAttr(name):
        assert type(name) == types.StringType, "ERROR: attribute must be of String type!"
        try:
            CfgLock.acquire()
            if GlobalCfg.__config.has_key(name):
                return GlobalCfg.__config[name]
            return None
        finally:
            CfgLock.release()

class Policy:
    defaults={
        'LOST_WORKER_TIMEOUT' : 10,
        'IDLE_WORKER_TIMEOUT' : 100
    }
    __policy = None

    def __init__(self, policy_path=None):
        Policy.__policy = dict([(k, v) for (k, v) in Policy.defaults.items()])
        # FIXME: need abs path
        if policy_path and os.path.exists(policy_path):
            #add parse config and set policy
            cf = ConfigParser.ConfigParser()
            cf.read(policy_path)
            if cf.has_section('Policy'):
                for key in cf.options('Policy'):
                    Policy.__policy[key] = cf.getint('Policy',key)

    @staticmethod
    def getPolicy(policy_path=None):
        if Policy.__policy is None:
            try:
                PolicyLock.acquire()
                if Policy.__policy is None:
                    Policy(policy_path)
            finally:
                PolicyLock.release()
        return Policy.__policy

    @staticmethod
    def setAttr(name, value):
        assert type(name) == types.StringType, "ERROR: attribute must be of String type!"
        try:
            PolicyLock.acquire()
            Policy.__policy[name] = value
        finally:
            PolicyLock.release()

    @staticmethod
    def getAttr(name):
        assert type(name) == types.StringType, "ERROR: attribute must be of String type!"
        try:
            PolicyLock.acquire()
            if Policy.__policy.has_key(name):
                return Policy.__policy[name]
            else:
                return None
        finally:
            PolicyLock.release()