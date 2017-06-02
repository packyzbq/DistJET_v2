#!/bin/bash

args=

while [ -n "$1" ]; do
    args="$args $1"
    shift
done

if [ -z $JUNOTESTROOT ];
then
    source  /afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Pre-Release/J17v1r1-Pre1/setup.sh
fi

python $JUNOTESTROOT/python/JunoTest/junotest UnitTest $args