from python.Application.IApplication import IApplication
import subprocess
import os


class UnitTestApp(IApplication):
    def __init__(self):
        IApplication.__init__(self)
        self.task_reslist = {}

    def split(self):
        # setup Offline software
        # FIXME: Can not source JUNO env in this process
        rc = subprocess.Popen(['./run.sh','list'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out,err = rc.communicate()
        case = out.split('\n')[2:-1]
        for c in case:
            self.data[c] = True
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
                    resfile.writelines(tasklist[id].getdata.keys()[0] + '   SUCCESS\n')
                else:
                    resfile.writelines(tasklist[id].getdata.keys()[0] + '   Error\n')


    def analyze_log(self, logname):
        with open(self.res_dir + '/result_' + str(logname)) as logfile:
            for line in logfile:
                if line.find('ERROR') != -1:
                    return False
                else:
                    with open(self.res_dir + '/error_' + str(logname), 'r+') as file:
                        if len(file.read()) == 0:
                            return True
                        else:
                            return False
