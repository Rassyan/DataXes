#!/usr/bin/env bash

mvn -U clean package assembly:assembly -Dmaven.test.skip=true

#tar -xf target/datax.tar.gz -C /opt

docker build -t rassyan/dataxes .