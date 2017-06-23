import os
import sys
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

if not os.environ['JUNOTOP']:
    print 'Do not detect JUNO environment, setup JUNO env'
    os.system('source /afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Pre-Release/J17v1r1-Pre2/setup.sh')
    

scripts = sys.argv[4]
if not os.path.exists(scripts):
    print 'Can not find analysis script, check path %s'%scripts
    exit()

tagdir = os.path.abspath(sys.argv[3])
if os.getcwd()!=tagdir:
    if os.path.exists(tagdir):
        os.chdir(tagdir)
    else:
        print 'Can not find tagdir, exit'
        exit()
ana_type = sys.argv[2]
if ana_type not in prepare_list_type.keys():
    print 'Unrecognized analysis type %s, exit'%ana_type
    exit()
if not os.path.exists('%s_ana' % ana_type):
    os.mkdir('%s_ana' % ana_type)
os.chdir('%s_ana' % ana_type)

if 'ana' in sys.argv[1].split('+'):
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
    print 'root -l -b -q %s+()'%scripts
    process = Process('root -l -b -q "%s+()"'%scripts,logfile)
    process.run()
    if process.returncode == 0:
        print 'analysis successfully'
    else:
        print 'analysis error,recode=%s, status=%s, skip cmp step'%(process.returncode,process.status)
        exit()

resfile_list=[]
for f in os.listdir(os.getcwd()):
    if f.endswith('.root'):
        resfile_list.append(f)

if 'cmp' in sys.argv[1].split('+'):
    # cmp step
    try:
        refdir = sys.argv[5]
    except IndexError:
        print 'No refpath input, skip cmp step skip'
        exit()

    if not os.path.exists(refdir):
        print 'Can not find refpath ,check path %s, exit'
        exit()

    plottest = PlotTester.PlotTester('%s_cmp.root'%ana_type,resfile_list,refdir)
    plottest.run()

