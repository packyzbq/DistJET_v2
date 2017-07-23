#!/usr/bin/env bash

junover="$1"
shift
args=

while [ -n "$1" ]; do
    args="$args $1"
	shift
done

if [ -z $JUNOTESTROOT ];
then
    source  "/afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/${junover}/setup.sh"
fi

bash -c ${args}
