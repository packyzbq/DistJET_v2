#!/bin/bash

if [ -z $JUNOTOP ]; then
    source /afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Pre-Release/J17v1r1-Pre2/setup.sh
fi

python analysis.py $*