import UTestApp
from python.IScheduler import SimpleScheduler
from python.Application.IAPPWorker import TestWorker

def run():
    app = UTestApp.UnitTestApp("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/Application/UnitTest/",'UnitTest')
    app.set_boot("run.sh")
    app.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/test/resdir")
    #app.set_worker(TestWorker)
    app.set_scheduler(SimpleScheduler)

    return app
