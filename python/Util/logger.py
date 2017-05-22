import logging
import os

from python.Util.Conf import Config

loglevel={'info':logging.INFO, 'debug':logging.DEBUG}


def getLogger(name, level=None):
    log_dir = Config.getCFGattr('Logdir')
    if not level:
        level = Config.getCFGattr('Log_Level')
        if not level:
            level = 'info'
    if not log_dir:
        log_dir = Config.getCFGattr('topdir')+'/log'
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    format = logging.Formatter('[%(asctime)s] %(threadName)s %(levelname)s: %(message)s')
    handler = logging.FileHandler(log_dir+'/DistJET.'+name+'.log')
    handler.setFormatter(format)

    logger = logging.getLogger('DistJET.'+name)
    logger.setLevel(loglevel[level])
    logger.addHandler(handler)
    return logger

def setlevel(level, logger=None):
    if logger:
        logger.setLevel(loglevel[level])
    Config.setCfg('Log_Level',level)