
HADOOP_HOME    = /opt/hadoop-1.0.4
HADOOP_CORE    = $(HADOOP_HOME)/hadoop-core-1.0.4.jar
HADOOP_START   = $(HADOOP_HOME)/bin/start-all.sh
HADOOP_STOP    = $(HADOOP_HOME)/bin/stop-all.sh
HADOOP_EXE     = $(HADOOP_HOME)/bin/hadoop
HADOOP_FORMAT  = $(HADOOP_EXE) namenode -format

JAVA_HOME      = /System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home/bin

idx_set_input:
	make -f Indexer/Makefile h_set_input

idx_get_output:
	make -f Indexer/Makefile h_get_output

indexing:
	make -f Indexer/Makefile exec

clustering:
	make -f Clusterizer/Makefile exec

h_start:
	$(HADOOP_START)

h_stop:
	$(HADOOP_STOP)

h_hdfs_r:
	rm -rd /opt/HDFS/*

h_format: h_hdfs_r
	$(HADOOP_FORMAT)

reset: h_stop h_format h_start idx_set_input

exec: indexing clustering

reset_exec: h_stop h_format h_start idx_set_input exec