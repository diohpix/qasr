
set JAVA_OPTS="-server  -Xmx2048m -Xms2048m -verbose:gc -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled "

java -server -cp ..\conf -Djava.ext.dirs=..\lib com.khalifa.Main
