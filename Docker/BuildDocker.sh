#! /bin/bash

MYDIR=$(dirname $(greadlink -f "$0"))
cd $MYDIR

git submodule update --init --recursive

docker build . -t 6.824
