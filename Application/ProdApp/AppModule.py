import ProdApp
from python.IScheduler import SimpleScheduler

def run(config_path=None):
    app = ProdApp.ProdApp('/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/Application/ProdApp','ProdApp',config_path)
    app.set_worker('ProdWorker')
    app.set_boot('run.sh')
    app.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/prodtest")
    app.set_scheduler(SimpleScheduler)
    return [app]