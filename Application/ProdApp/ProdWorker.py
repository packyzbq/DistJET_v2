import sys
sys.path.append('/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2')
from python.Application.IAPPWorker import IAPPWorker
from python.Process import Process
import os
class ProdWorker(IAPPWorker):
    def initialize(self,**kwargs):
        pass

    def finalize(self, **kwargs):
        pass

    def do_work(self,boot=None,data={},args={},resdir=None,log=None):
        #data= 0.sample,1.subdir,2.tag,3.seed,4.workflow_list
        self.log.debug('[ProdWorker] start run task....')
        # FIXME: env mpi need python2.6, python change to 2.7 after source juno env,so cannot run mpi
        #integrate source juno and run bash
        data_value = data.values()[0]
        data_path =''
        data_path+=resdir
        flag = True
        for i in range(3):
            if os.path.exists(data_path):
                if data_value[i] is not None and data_value[i] in os.listdir(data_path):
                    data_path=data_path+'/'+data_value[i]
            else:
                self.log.error('[ProdApp] Can not find directory=%s'%data_path)
                flag = False
                break
        if not flag:
            self.log.error('[ProdApp] task error,exit')
            return 1
        exec_path =[]
        for step in data_value[4]:
            if step in os.listdir(data_path):
                for dd in os.listdir(data_path+'/'+step):
                    if dd.startswith('run') and dd.endswith(str(data_value[3])+'.sh'):
                            exec_path.append(data_path+'/'+step+'/'+dd)
            else:
                self.log.warning('[ProdApp] work step %s has no corresponding run scripts, current work flow path=%s'
                                 %(step,os.listdir(data_path)))
        if len(exec_path)!=0:
            self.log.debug('[ProdApp] Running scripts = %s'%exec_path)
            process = Process(exec_path)
            ret = process.run()
            self.log.info('[ProdApp] task %s finish'%data.values()[0])
            return ret
        else:
            self.log.warning('[ProdApp] No task script detected, exit with error')
            return 1

if __name__ == '__main__':
    import logging
    log = logging.getLogger('testlog')
    log.addHandler(logging.FileHandler('testlog.log'))
    log.addHandler(logging.StreamHandler())
    worker = ProdWorker(log)
    ret = worker.do_work(None,{1:['Positron-ch2-uniform','uniform','e+_3.0MeV',64,['detsim','elecsim','calib','rec']]},
                   resdir='/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/Application/ProdApp/test')
    print ret
