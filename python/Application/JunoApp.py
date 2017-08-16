from IApplication import IApplication
import os
preRdir = '/afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Pre-Release'
Rdir = '/afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Release'
class JunoApp(IApplication):
    def __init__(self, rootdir, name, config_path=None,JunoVER=None):
        super(rootdir,name,config_path=config_path)
        self.JunoVer = self.setJunoVer(JunoVER)
        self.flag = "JUNO"

    def setJunoVer(self,JunoVer):
        if JunoVer is not None:
            self.JunoVer = JunoVer
        else:
            try:
                PreR_verlist = os.listdir('/afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Pre-Release')
                R_verlist = os.listdir('/afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Release')
                R_v = R_verlist.sort()[-1]
                PreR_verlist = PreR_verlist.sort()
                for i in range(1,len(PreR_verlist)):
                    if 'Pre' in PreR_verlist[-i]:
                        Pre_V = PreR_verlist[-i]
                        break
                self.JunoVer = R_v if R_v > Pre_V else Pre_V
            except Exception:
                self.log.error('[JunoApp] Error occur when access juno directory, use default juno version: J17v1r1')
                self.JunoVer = 'J17v1r1'

    def setup(self):
        if 'Pre' in self.JunoVer:
            command = preRdir +'/'+self.JunoVer+'/setup.sh'
        else:
            command = Rdir + '/' + self.JunoVer + '/setup.sh'
        if not os.path.exists(command):
            self.log.error('[JunoApp] Error when setup, can not find setup.sh in path:%s'%command)
            return None
        else:
            return ["source %s"%command]

