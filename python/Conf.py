import types
import ConfigParser

class AppConf:
    pass

class GlobalCfg:
    def getCfg(self):
        return self

class Policy:
    LOST_WORKER_TIMEOUT = 10
    IDLE_WORKER_TIMEOUT = 100
    pass