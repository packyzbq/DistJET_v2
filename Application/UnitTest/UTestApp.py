from python.Application.JunoApp import JunoApp
import subprocess
import os


class UnitTestApp(JunoApp):
    def __init__(self, rootdir, name):
        JunoApp.__init__(self, rootdir, name)
        self.task_reslist = {}

    def split(self):
        # setup Offline software
        # FIXME: Can not source JUNO env in this process
        print self.rootdir
        script = self.rootdir+'/run.sh'
        rc = subprocess.Popen([script,"list"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out,err = rc.communicate()
        if err:
            self.log.error('[APP_%d] @split() error = %s'%(self.id,str(err)))
        case = out.split('\n')
        startline = 0
        for line in case:
            startline+=1
            if 'unittest cases' in line:
                break
        for c in case[startline:]:
            if c != '':
                self.data[c] = ""
        self.log.info('split data = %s'%self.data)
        self.setStatus('data')
        return self.data


    def merge(self, tasklist):
        """
        :param data:
        :param tasklist: {tid:task}
        :return:
        """

        for id in tasklist.keys():
            if self.analyze_log(id):
                self.task_reslist[id] = True
            else:
                self.task_reslist[id] = False
            with open(self.res_dir+'/summary.log', 'a+')as resfile:
                if self.task_reslist[id]:
                    resfile.writelines(tasklist[id].getdata().keys()[0] + '   SUCCESS\n')
                else:
                    resfile.writelines(tasklist[id].getdata().keys()[0] + '   Error\n')


    def analyze_log(self, logname):
        logpath = self.res_dir+'/app_%s_task_%s'%(self.id,logname)
        if not os.path.exists(logpath):
            self.log.error('[%s] error occurs when analyze log file %s'%(self.name,logpath))
            return False
        with open(logpath, 'a+') as logfile:
            self.log.debug('Parse log file %s'%logpath)
            for line in logfile:
                if line.find('ERROR') != -1:
                    self.log.info('Find ERROR in log file, task fail')
                    return False
                else:
                    return True
                    #with open(self.res_dir + '/error_' + str(logname), 'r+') as file:
                    #    if len(file.read()) == 0:
                    #        return True
                    #    else:
                    #        return False
        
if __name__ == '__main__':
    app = UnitTestApp()
