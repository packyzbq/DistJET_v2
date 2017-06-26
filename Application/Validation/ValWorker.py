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


