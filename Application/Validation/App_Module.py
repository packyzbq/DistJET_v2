import ValApp
def run(config_path):
    app = ValApp.ValApp('$DistJETPATH/Application/Validation/','ValApp',config_path)
    app.set_boot('analysis.py')
    app.set_resdir("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/test/valtest/resdir")
    return app
