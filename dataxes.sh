#!/usr/bin/env bash

if [ -z "$1" ]; then
    echo "No job name parameter, run JDBC Job Tool."
    docker run --rm -it -v $(pwd):/opt/dataxes/ rassyan/dataxes python /opt/datax/bin/jdbc_job_tool.py
    exit 0
fi

suffix=`date "+%Y%m%d%H%M%S"`
dataxes_job_name=$1
dataxes_args=`echo "$@" | awk '{for(i=2;i<=NF;i++)printf $i" "}'`
if [ "-d" == "$1" ]; then
    docker_args=$1
    dataxes_job_name=$2
    dataxes_args=`echo "$@" | awk '{for(i=3;i<=NF;i++)printf $i" "}'`
fi

docker run --rm ${docker_args} \
-v $(pwd):/opt/dataxes/ \
--name dataxes_${dataxes_job_name//@/_}_${suffix} \
rassyan/dataxes \
python -u ${dataxes_job_name}.py ${dataxes_args}