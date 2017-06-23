import os
import sys
from subprocess import PIPE,Popen,STDOUT
from python.Process import Process
import PlotTester

# argv[1]=[ana,cmp] argv[2]=types(detsim...) argv[3]=tagdir path(abspath) argv[4]=scripts_path argv[5]=refpath

prepare_list_type={'detsim':'user-detsim',
                   'elecsim':'user-elecsim',
                   'rec':'rec'}

def add_prepare(key, val):
    prepare_list_type[key] = val

if not os.environ['JUNOTOP']:
    print 'Do not detect JUNO environment, exit'
    exit()

scripts = sys.argv[4]
if not os.path.exists(scripts):
    print 'Can not find analysis script, check path %s'%scripts
    exit()

if os.getcwd()!=sys.argv[3]:
    if os.path.exists(sys.argv[3]):
        os.chdir(sys.argv[3])
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

if 'ana' in sys.argv[1]:
    # prepare list of *.root
    rc = Popen(['find $(pwd) -name %s*.root | sort -n > lists_%s.txt'%(prepare_list_type[ana_type],ana_type)],stdout=PIPE,stderr=PIPE,shell=True)
    out,err = rc.communicate()
    if err:
        print 'prepare list of %s error, terminate'%ana_type

    # analysis
    logfile = '%s_ana.log'%ana_type
    process = Process('root -l -b -q %ss+()'%scripts,logfile)
    process.run()
    if process.returncode == 0:
        print 'analysis successfully'
    else:
        print 'analysis error, skip cmp step'
        exit()

resfile_list=[]
resfile_list.append(os.listdir(os.getcwd()))

if 'cmp' in sys.argv[1]:
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

