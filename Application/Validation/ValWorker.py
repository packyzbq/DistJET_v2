from python.Application.IAPPWorker import IAPPWorker
from python.Process import Process
import types

class ValWorker(IAPPWorker):
    def initialize(self,**kwargs):
        self.log.info('[ValWorker] Initialized...')
        return 0

    def finalize(self, **kwargs):
        self.log.info('[ValWorker] Finalized...')
        return 0

    def do_work(self,boot=None,data={},args={},resdir=None,log=None,extra=None):
        '''
        :param data: id:{"workdir":os.path.abspath(tagdir),'step':anastep,
                     'anaflag':self.cfg.anaflag,'cmpflag':self.cfg.cmpflag,
                     'refpath':self.cfg.refpath,'anascript':}
        :param args:
        :param resdir:
        :param extra:
        :return:
        '''
        self.log.debug('[ValWorker] start run task....')
        parg = ''
        key = data.keys()[0]
        if data[key]['anaflag']=='True' and data[key]['cmpflag']=='True':
            parg+='ana+cmp'
        elif data[key]['anaflag']=='True':
            parg+='ana'
        elif data[key]['cmpflag']=='True':
            parg+='cmp'

        parg+=' %s'%data[key]['step'].encode('utf-8')
        parg+=' %s'%data[key]['workdir'].encode('utf-8')
        parg+=' %s'%data[key]['anascript'].encode('utf-8')
        parg+=' %s'%data[key]['refpath'].encode('utf-8')
        if type(boot) == types.ListType:
            boot = boot[0]      
        if boot.endswith('.sh'):
            exe = 'bash'
        elif boot.endswith('.py'):
            exe = 'python'
        self.log.info('[ValWorker] run script: %s %s %s'%(exe,boot,parg))
        process = Process('%s %s %s'%(exe,boot,parg),log,ignoreFail=True)
        recode = process.run()
        self.log.info('[ValWorker] task finish')
        return recode


