from TestApp import TestApp
from python.IScheduler import SimpleScheduler

def run(conf_path=None):
    app=[]
    app1 = TestApp("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/Application/TestApp/","TestApp")
    app1.set_boot("executor.py")
    app1.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/test/tapp/TestApp_res")
    app1.set_scheduler(SimpleScheduler)

    app2 = TestApp("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/Application/TestApp/","TestApp")
    app2.set_boot("executor.py")
    app2.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/test/tapp/TestApp2_res")
    app2.set_scheduler(SimpleScheduler)

    app.append(app1)
    app.append(app2)
    return app
