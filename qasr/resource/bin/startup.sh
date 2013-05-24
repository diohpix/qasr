#!/bin/bash
export JAVA_OPTS="-server  -Xmx2048m -Xms2048m -verbose:gc -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseFastAccessorMethods"
$JAVA_HOME/bin/java $JAVA_OPTS -cp "../conf:$(echo ../lib/*.jar | tr ' ' ':')"  com.qasr.Main 2>&1 > /tmp/log &