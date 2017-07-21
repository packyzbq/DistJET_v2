import sys
sys.path.append('/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2')
from python.Application.IApplication import IApplication
from python.Util.Conf import AppConf
import os


def MakeandCD(path):
    if not os.path.exists(path):
        os.mkdir(path)
    os.chdir(path)

class ProdApp(IApplication):
    def __init__(self,rootdir,name,config_path=None):
        IApplication.__init__(self,rootdir=rootdir,name=name)
        self.workflow = []
        self.driver_dir=[]
        self.driver={} #driver name: driver scripts list
        self.sample_list=[]

        if not config_path or not os.path.exists(os.path.abspath(config_path)):
            self.log.warning('[ProdApp] Can not find config file = %s, use default'%config_path)
            config_path = os.environ['DistJETPATH']+'/Application/ProdApp/config.ini'
        self.cfg = AppConf(config_path,'ProdApp')
        if self.cfg.get('topDir'):
            self.topdir=self.cfg.get('topDir')
            self.status['topdir']=True
        self.sample_list.extend(self.cfg.getSections())
        self.JunoTopDir = '/afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6'
        self.JunoVer = self.cfg.get('junover')
        print self.JunoVer
        if 'Pre' in self.JunoVer:
            self.JunoTopDir+='/Pre-Release/'+self.JunoVer
        else:
            self.JunoTopDir+='/Release/'+self.JunoVer

    
    def getcfgattr(self,item,sec=None):
        if self.cfg.get(item,sec):
            return self.cfg.get(item)
        else:
            return None

    def split(self):
        os.chdir(self.res_dir)
        self._find_driver_script()
        self._generate_job_bash()


    def merge(self, tasklist):
        pass

    def _find_driver_script(self,driver_name=None):
        """
        :param driver_name:
        :return: self.driver[driver_name] = {scp1: scp1_path, scp2:scp2_path...}
        """
        # position the dirver dir
        top_driver_dir = self.JunoTopDir+'/offline/Validation/JunoTest/production'
        user_extra_dir=[]
        user_extra_dir_str = os.getenv("JUNOTESTDRIVERPATH")
        if user_extra_dir_str:
            user_extra_dir.extend(user_extra_dir_str.split(":"))
        user_extra_dir.append(top_driver_dir)
        # get driver list
        for dd in user_extra_dir:
            print "searching on %s"%dd
            # check directory exists or not
            if not os.path.exists(dd):
                self.log.warning("WARN: %s does not exist" % dd)
                continue
            if not os.path.isdir(dd):
                self.log.warning("WARN: %s is not a directory" % dd)
                continue
            # get the contents of dd
            for d in os.listdir(dd):
                path = os.path.join(dd, d)
                if not os.path.isdir(path): continue
                # d is driver
                if self.driver.has_key(d):
                    self.log.warning("WARN: %s (%s) already exists, skip %s" % (d, str(self.driver[d]), path))
                    continue

                # if the script is already in PathA, use it.
                scripts = self.driver.get(d, [])
                scripts_base = {}
                for f in scripts:
                    scripts_base[os.path.basename(f)] = f
                for f in os.listdir(path):
                    # only match prod_*
                    if not f.startswith('gen'): continue
                    # if the script is already added, skip it.
                    if scripts_base.has_key(f): continue
                    scripts_base[f] =os.path.join(path,f)
                    #scripts.append(os.path.join(path, f))
                    # print('element:%s add script %s'%(d,os.path.join(path,f)))
                if len(scripts_base):
                    self.driver[d] = scripts_base
        if driver_name:
            return self.driver.get(driver_name)

    def _generate_job_bash(self):
        """
        :param cfg: extraArgs,workDir,workSubDir,external-input-dir,benchmark
        :return:
        """
        args=None
        for sample in self.sample_list:
            chain_script = self.driver.get(self.cfg.get('driver',sec=sample))
            if not chain_script:
                self.log.warning('WARN: Can not find specify driver: %s for sample:%s, skip'%(self.cfg.get('driver',sec=sample),sample))
                continue
            # check specify script
            scripts = self.cfg.get('scripts',sec=sample).split(' ')
            if not scripts:
                scripts = chain_script
            elif not set(chain_script.keys()) > set(scripts):
                self.log.warning("WARN: Can not find specified scripts: %s in driver: %s, skip"%(scripts,self.cfg.get('driver',sec=sample)))
            MakeandCD(sample)
            # generate directory structure and run gen_bash script
            tags = self.cfg.get('tags',sec=sample)
            workflow = self.cfg.get('workflow',sec=sample)
            for spt in scripts:
                back_dir = os.getcwd()
                worksubdir=None
                if 'uniform' in spt:
                    MakeandCD('uniform')
                    worksubdir = 'uniform'
                elif 'center' in spt:
                    MakeandCD('center')
                    worksubdir='center'
                for step in workflow.split(' '):
                    args = self._gen_args(sample, worksubdir=worksubdir)
                    args+=' %s'%step
                    self.log.info('bash %s %s'%(spt,args))
                    os.system('bash %s %s'%(chain_script[spt],args))
                os.chdir(back_dir)





    def _gen_args(self,sample,worksubdir=None):
        args=''
        arg_list = ['seed','evtmax','njobs']
        arg_neg_list = ['driver','scripts','workflow']
        args += ' --setup "$JUNOTOP/setup.sh"'
        for k,v in self.cfg.config.items():
            if k in arg_list:
                args+=' --%s "%s"'%(k,v)
        for k,v in self.cfg.other_cfg[sample].items():
            if k not in arg_neg_list:
                print '%s:%s'%(k,v)
                args+=' --%s "%s"'%(k,v)
        if not self.cfg.get('worksubdir',sample) and worksubdir:
            args+=' --worksubdir "%s"'%worksubdir
        return args

    
if __name__ == '__main__':
    app = ProdApp('/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2/Application/ProdApp','ProdApp')
    resdir = app.rootdir+'/test'
    print('resdir = %s'%resdir)
    app.set_resdir(resdir)
    app.split()
