#!/bin/bash
export JAVA_HOME="/home/interest/install/java"
/usr/bin/killall -9 java
sleep 1
cd /home/interest/server/dbproxy/bin
export JAVA_OPTS="-server  -Xmx2048m -Xms2048m -verbose:gc -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseFastAccessorMethods"
$JAVA_HOME/bin/java $JAVA_OPTS -cp "../conf:$(echo ../lib/*.jar | tr ' ' ':')"  com.khalifa.Main 2>&1 > /tmp/log &