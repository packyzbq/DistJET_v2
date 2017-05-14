import types
import ConfigParser

class AppConf:
    pass

class GlobalCfg:
    defaults={
        'health_detect_scripts': None,
        'topDir': None
    }

    def __init__(self):
        self.config = dict([(k,v) for (k,v) in GlobalCfg.defaults.items()])

    def getCfg(self):
        return self.config

    def update(self,**kwd):
        self.config.update(kwd)

    def setAttr(self, name, value):
        assert type(name) == types.StringType, "ERROR: attribute must be of String type!"
        self.config[name] = value
    def getAttr(self, name):
        if self.config.has_key(name):
            return self.config[name]
        return None

class Policy:
    LOST_WORKER_TIMEOUT = 10
    IDLE_WORKER_TIMEOUT = 100
    pass