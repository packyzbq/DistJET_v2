import UTestApp
from python.IScheduler import SimpleScheduler

def run():
    app = UTestApp.UnitTestApp()
    app.set_boot("./run.sh")
    app.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET/result")
    app.set_scheduler(SimpleScheduler)
    return app