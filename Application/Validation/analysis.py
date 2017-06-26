import os
import sys

if not os.environ.has_key('JUNOTOP'):
    print 'Please source JUNO Env first'
    exit()
else:
    print os.environ['JUNOTOP']
print sys.argv
from subprocess import PIPE,Popen,STDOUT
sys.path.append("/afs/ihep.ac.cn/users/z/zhaobq/workerSpace/DistJET_v2")
from python.Process import Process
import PlotTester
# argv[1]=[ana,cmp] argv[2]=types(detsim...) argv[3]=tagdir path(abspath) argv[4]=scripts_path argv[5]=refpath

prepare_list_type={'detsim':'user-detsim',
                   'elecsim':'user-elecsim',
                   'rec':'rec'}

def add_prepare(key, val):
    prepare_list_type[key] = val



tagdir = sys.argv[3].encode('utf-8')
print 'tagdir=%s, pwd=%s'%(tagdir,os.getcwd())
print tagdir
if os.getcwd()!=tagdir:
    if os.path.exists(tagdir):
        os.chdir(tagdir)
    else:
        print 'Can not find tagdir=%s, exit'%tagdir
        exit()
ana_type = sys.argv[2]
if ana_type not in prepare_list_type.keys():
    print 'Unrecognized analysis type %s, exit'%ana_type
    exit()
if not os.path.exists('%s_ana' % ana_type):
    os.mkdir('%s_ana' % ana_type)
#os.chdir('%s_ana' % ana_type)

if 'ana' in sys.argv[1].split('+'):
    scripts = sys.argv[4]
    if not os.path.exists(scripts):
        scripts=os.environ['TUTORIALROOT']+'/'+scripts
        print scripts
        if not os.path.exists(os.path.abspath(scripts)):
            print 'Can not find analysis script, check path %s'%scripts
            exit()
   # prepare list of *.root
    print 'find %s -name %s*.root | sort -n > lists_%s.txt'%(tagdir,prepare_list_type[ana_type],ana_type)
    rc = Popen(['find %s -name %s*.root | sort -n > lists_%s.txt'%(tagdir,prepare_list_type[ana_type],ana_type)],stdout=PIPE,stderr=PIPE,shell=True)
    out,err = rc.communicate()
    if err:
        print 'prepare list of %s error,errmsg=%s, terminate'%(ana_type,err)
    listfile = 'lists_%s.txt'%ana_type
    if not os.path.exists(listfile) or not os.path.getsize(listfile):
        print 'prepare list is not exist or empty, exit'
        exit()
    # analysis
    logfile = '%s_ana.log'%ana_type
    logFile = open(logfile,'w+')
    print 'root -l -b -q %s+()'%scripts
    process = Process('root -l -b -q "%s+()"'%scripts,logFile)
    process.run()
    logFile.flush()
    logFile.close()
    if process.returncode == 0:
        print 'analysis successfully'
    else:
        print 'analysis error,recode=%s, status=%s, skip cmp step'%(process.returncode,process.status)
        exit()


if 'cmp' in sys.argv[1].split('+'):
    # cmp step
    os.chdir('%s_ana' % ana_type)
    resfile_list=[]
    for f in os.listdir(os.getcwd()):
        if f.endswith('.root'):
            resfile_list.append(f)
    print 'resfile list = %s'%resfile_list

    try:
        refdir = os.path.abspath(sys.argv[5])
    except IndexError:
        print 'No refpath input, skip cmp step skip'
        exit()

    if not os.path.exists(refdir):
        print 'Can not find refpath ,check path %s, exit'%refdir
        exit()

    plottest = PlotTester.PlotTester('%s_cmp.root'%ana_type,resfile_list,refdir)
    plottest.run()

