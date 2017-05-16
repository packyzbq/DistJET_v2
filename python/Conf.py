import types
import threading
import os
import ConfigParser

PolicyLock = threading.RLock()
CfgLock = threading.RLock()

class AppConf:
    __cfg = {
        'appid':None,
        'appName': None,
        'workDir': None,
        'topDir': GlobalCfg.getAttr('topDir')
    }

    load_flag = False
    load_lock = threading.RLock()


    @staticmethod
    def loadAppCfg(cfg_path = None):
        # FIXME: Need abs path
        try:
            AppConf.load_lock.acquire()
            if cfg_path and os.path.exists(cfg_path):
                cf = ConfigParser.ConfigParser()
                cf.read(cfg_path)
                if cf.has_section('AppCfg'):
                    for key in cf.options('AppCfg'):
                        AppConf.__cfg[key] = cf.get('AppCfg', key)
            AppConf.load_flag = True
        finally:
            AppConf.load_lock.release()

        return AppConf.__cfg

    @staticmethod
    def __getattr__(item):
        assert type(item) == types.StringType, "ERROR: attribute must be of String type!"
        if AppConf.__cfg.has_key(item):
            return AppConf.__cfg[item]
        else:
            return None

    @staticmethod
    def __setattr__(key, value):
        assert type(key) == types.StringType, "ERROR: attribute must be of String type!"
        AppConf.__cfg[key] = value


class GlobalCfg:
    __config={
        'health_detect_scripts': None,
        'topDir': None
    }

    @staticmethod
    def loadCfg(cfg_path=None):
        # FIXME: Need abs path
        if cfg_path and os.path.exists(cfg_path):
            cf = ConfigParser.ConfigParser()
            cf.read(cfg_path)
            if cf.has_section('Cfg'):
                for key in cf.options('Cfg'):
                    GlobalCfg.__config[key] = cf.get('Cfg', key)
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
    __policy={
        'LOST_WORKER_TIMEOUT' : 10,
        'IDLE_WORKER_TIMEOUT' : 100,
        'CONTROL_DELAY'       : 1,
        'ATTEMPT_TIME'        : 2
    }

    @staticmethod
    def loadPolicy(policy_path=None):
        # FIXME: need abs path
        if policy_path and os.path.exists(policy_path):
            # add parse config and set policy
            cf = ConfigParser.ConfigParser()
            cf.read(policy_path)
            if cf.has_section('Policy'):
                for key in cf.options('Policy'):
                    Policy.__policy[key] = cf.getint('Policy',key)
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
                return 1
        finally:
            PolicyLock.release()