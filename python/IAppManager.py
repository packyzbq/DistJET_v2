import Queue
from Util import logger

import Task

appmgr_log = logger.getLogger('AppMgr')

class IAppManager:
    def __init__(self, apps):
        self.applist={}     # A list of applications  id:app
        #self.task_queue = Queue.Queue() # tid:task
        self.task_list = {} # tid: task
        self.__tid = 0
        index = 0
        for app in apps:
            self.applist[index] = app
            app.set_id(index)
            index+=1
        appmgr_log.debug('[AppMgr] Load apps, the number of app = %d'%len(apps))
        self.current_app = self.applist[0]
        self.current_app_id = 0
    def create_task(self,app=None):
        """
        According to split function of app, split data and create small tasks, store them into task_queue
        :param app:
        :return:
        """
        raise NotImplementedError

    def get_current_appid(self):
        return self.current_app_id

    def get_current_app(self):
        return self.applist[self.current_app_id]

    def finalize_app(self,app=None):
        """
        The application operations when all tasks are finished
        :return:
        """
        raise NotImplementedError

    def next_app(self):
        """
        Start the next application
        :return: application
        """
        raise NotImplementedError

    def get_app_task_list(self, app=None):
        if not app:
            return self.task_list
        if len(self.task_list)==0 or app == self.current_app:
            self.create_task(app)
        return self.task_list

    def get_task(self, tid):
        return self.task_list[tid]

    def get_app_init(self, appid):
        if not appid in self.applist.keys():
            appmgr_log.error('@get app initialize boot error, can not find appid=%d'%appid)
            return None
        app = self.applist[appid]
        return dict({'boot':app.app_init_boot, 'resdir': app.res_dir},**app.app_init_extra)

    def get_app_fin(self, appid):
        if not appid in self.applist.keys():
            appmgr_log.error('@get app initialize boot error, can not find appid=%d'%appid)
            return None
        app = self.applist[appid]
        return dict({'boot':app.app_fin_boot, 'resdir': app.res_dir},**app.app_fin_extra)

class SimpleAppManager(IAppManager):

    def create_task(self, app=None):
        if not app:
            app = self.applist[self.current_app_id]
        data = app.split()
        for k,v in data:
            # create tasks, and store in task_queue
            task = Task.Task(self.__tid)
            self.__tid+=1
            task.initial(app.app_boot, app.args, {k:v}, app.res_dir)
            #self.task_queue.put(task)
            self.task_list[task.tid] = task
        appmgr_log.info('[AppMgr] App %d, Create %d tasks'%(app.id,len(data)))

    def finalize_app(self, app=None):
        if not app:
            app = self.applist[self.current_app_id]
        app.merge(self.get_app_task_list(app))

    def next_app(self):
        if self.current_app_id != len(self.applist)-1:
            self.current_app_id += 1
            return self.applist[self.current_app_id]
        else:
            return None
