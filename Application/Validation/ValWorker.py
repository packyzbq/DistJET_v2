from python.Application.IAPPWorker import IAPPWorker
from python.Process import Process
import types

class ValWorker(IAPPWorker):
    def initialize(self,**kwargs):
        pass

    def finalize(self, **kwargs):
        pass

    def do_work(self,boot=None,data={},args={},resdir=None,log=None):
        '''
        :param data: id:{"workdir":os.path.abspath(tagdir),'step':anastep,
                     'anaflag':self.cfg.anaflag,'cmpflag':self.cfg.cmpflag,
                     'refpath':self.cfg.refpath,'anascript':}
        :param args:
        :param resdir:
        :param extra:
        :return:
        '''
        parg = ''
        key = data.keys()[0]
        if data[key]['anaflag'] and data[key]['cmpflag']:
            parg+='ana+cmp'
        elif data[key]['anaflag']:
            parg+='ana'
        elif data[key]['cmpflag']:
            parg+='cmp'

        parg+=' %s'%data[key]['step']
        parg+=' %s'%data[key]['workdir']
        parg+=' %s'%data[key]['anascript']
        parg+=' %s'%data[key]['refpath']
        if type(boot) == types.ListType:
            boot = boot[0]
        process = Process('python %s %s'%(boot,parg),log)
        return process.run()


if __name__ == '__main__':
    import os
    os.environ['DistJETPATH']='/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2'
    with open('valworker_test.log','w+') as logfile:
        worker = ValWorker(logfile)
        data = {"workdir":'/junofs/production/validation/J17v1r1-Pre2/zhaobq/test/datatest','step':'detsim',
                'anaflag':True,'cmpflag':True,
                'refpath':'/junofs/production/validation/J17v1r1-Pre2/zhaobq/test/otherVersion',
                'anascript':'$TUTORIALROOT/share/SimAnalysis/drawDetSim.C'}
        recode = worker.do_work(boot=os.environ['DistJETPATH']+'/Application/Validation/analysis.py',data=data,log=logfile)
        print recode
