import UTestApp
from python.IScheduler import SimpleScheduler
from python.Application.IAPPWorker import TestWorker

def run():
    app = UTestApp.UnitTestApp()
    app.set_boot("./run.sh")
    app.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/test")
    app.set_scheduler(SimpleScheduler)

    return app