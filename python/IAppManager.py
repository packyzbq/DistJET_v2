import logging
import IApplication
import Queue
import Task

appmgr_log = logging.getLogger('AppMgr')

class IAppManager:
    def __init__(self, apps):
        self.applist={}     # A list of applications  id:app
        self.current_app_id = 0
        self.task_queue = Queue.Queue()
        self.task_list = {} # tid: task
        index = 0
        for app in apps:
            self.applist[index] = app
            index+=1

    def create_task(self,app):
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

    def finalize_app(self):
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

    def get_app_task_list(self):
        if self.task_queue.empty():
            self.create_task(self.get_current_app())
        return self.task_queue

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

    def create_task(self,app):
        data = app.split()
        for k,v in data:
            # create tasks, and store in task_queue
            task = Task.Task()
            task.initial(app.app_boot, app.args, {k:v}, app.res_dir)
            self.task_queue.put(task)
            self.task_list[task.tid] = task

    def finalize_app(self):
        #TODO do merge
        pass

    def next_app(self):
        if self.current_app_id != len(self.applist)-1:
            self.current_app_id += 1
            return self.applist[self.current_app_id]
        else:
            return None