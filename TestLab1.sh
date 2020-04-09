#! /bin/bash

bash Docker/BuildDocker.sh

docker run --rm -it -v $PWD/write-up:/root/6.824/write-up 6.824 sh -c \
"cp ./write-up/lab1/*.go ./src/mr/ \
&& cd src/main/ && bash test-mr.sh"
