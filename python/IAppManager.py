import Queue
from Util import logger
import Task

appmgr_log = logger.getLogger('AppMgr')

class IAppManager:
    def __init__(self, apps):
        self.applist={}     # A list of applications  id:app
        self.app_status = {} # appid: true/false
        #self.task_queue = Queue.Queue() # tid:task
        self.task_list = {} # tid: task
        self.app_task_list = {} # appid: task_list(list of task obj)
        self.tid = 0
        self.runflag = False
        index = 0
        for app in apps:
            if not app.checkApp():
                appmgr_log.warning('[AppMgr] APP %s is incompatible, skip'%app.name)
                continue
            self.applist[index] = app
            self.app_status[index] = False
            app.set_id(index)
            index+=1
        appmgr_log.debug('[AppMgr] Load apps, the number of app = %s'%self.applist)
        if len(self.applist) > 0:
            self.current_app = self.applist[0]
            self.current_app_id = 0
            self.runflag = self.gen_task_list()
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
        self.app_status[app.id] = True

    def next_app(self):
        """
        Start the next application
        :return: application
        """
        raise NotImplementedError

    def get_app_task_list(self, app=None):
        if not app:
            app = self.current_app
        if app.id not in self.app_task_list.keys():
            self.gen_task_list(app)
        return self.app_task_list[app.id]

    def get_task(self, tid):
        return self.task_list[int(tid)]

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

    def gen_task_list(self, app=None):
        if not app:
            app = self.applist[self.current_app_id]
        tmp_tasklist = self.create_task(app)
        if tmp_tasklist:
            self.app_task_list[app.id] = tmp_tasklist
            appmgr_log.info('[AppMgr] App %d, Create %d tasks'%(app.id,len(self.app_task_list[app.id])))
            return True
        else:
            return False

class SimpleAppManager(IAppManager):

    def create_task(self, app=None):
        data = app.split()
        app.log.debug('after split')
        for k,v in data.items():
            # create tasks, and store in task_queue
            task = Task.Task(self.tid)
            self.tid+=1
            task.initial(app.app_boot, app.args, {k:v}, app.res_dir)
            #self.task_queue.put(task)
            self.task_list[task.tid] = task
        if len(self.task_list) == 0 and len(data) == 0:
            appmgr_log.error('[AppMgr]: Create 0 task, check app split() method')
            return None
        else:
            return self.task_list
        

    def finalize_app(self, app=None):
        if not app:
            app = self.applist[self.current_app_id]
        if not self.app_status[app.id]:
            appmgr_log.info('[AppMgr] App %s finalizing'%app.name)
            appmgr_log.debug('[AppMgr] AppMgr merge tasks= %s'%self.get_app_task_list(app))
            app.merge(self.get_app_task_list(app))
            IAppManager.finalize_app(self,app)
        
    def next_app(self):
        if self.current_app_id != len(self.applist)-1:
            self.current_app_id += 1
            return self.applist[self.current_app_id]
        else:
            return None
