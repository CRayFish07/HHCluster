
###############################################################################
# HADOOP WORDCOUNT - ALBERTO HUELAMO SEGURA                                   #
#                                                                             #
# *Before building...                                                         #
# Make sure /Users/alberto/Desktop/testWordCount                              #
#           /Users/alberto/Desktop/testWordCount/input                        #
#           /Users/alberto/Desktop/testWordCount/output dirs are created.     #
# -Build:                                                                     #
# $ make                                                                      #
#                                                                             #
# *Before running...                                                          #
# $ make h_format                                                             #
# $ make h_start                                                              #
#                                                                             #
# -Run:                                                                       #
# $ make run                                                                  #
###############################################################################

HADOOP_HOME    = /opt/hadoop-1.0.4
HADOOP_CORE    = $(HADOOP_HOME)/hadoop-core-1.0.4.jar
HADOOP_START   = $(HADOOP_HOME)/bin/start-all.sh
HADOOP_STOP    = $(HADOOP_HOME)/bin/stop-all.sh
HADOOP_EXE     = $(HADOOP_HOME)/bin/hadoop
HADOOP_FORMAT  = $(HADOOP_EXE) namenode -format


LOGGER         = /Users/alberto/direct/tfg_repo/lib/log4j-1.2.17.jar

JAVA_HOME      = /System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home/bin

JAVAC          = $(JAVA_HOME)/javac
JAR            = $(JAVA_HOME)/jar

CLASSPATH      = $(HADOOP_CORE):$(LOGGER)

SRC_DIR        = /Users/alberto/direct/tfg_repo/Indexer/src
SOURCES        = $(SRC_DIR)/edu/ub/ahstfg/indexer/wordcount/*.java

JAVA_DIR       = /Users/alberto/Desktop/testWordCount/java
JAR_DIR        = ~/Desktop/testWordCount/Indexer.jar

MAIN_CLASS     = edu.ub.ahstfg.indexer.Indexer
ARGS           = input output

INPUT_DIR      = ~/Desktop/testWordCount/input
INPUT_FILES    = /*.arc.gz
OUTPUT_DIR     = ~/Desktop/testWordCount/

RUN            = $(HADOOP_EXE) jar $(JAR_DIR) $(ARGS)

all: clean jar

mkdirs:
	mkdir $(JAVA_DIR)

javac: mkdirs
	$(JAVAC) -classpath $(CLASSPATH) -d $(JAVA_DIR) $(SOURCES)

jar: javac
	$(JAR) -cvf $(JAR_DIR) -C $(JAVA_DIR)/ .

h_start:
	$(HADOOP_START)

h_stop:
	$(HADOOP_STOP)

h_hdfs_r:
	rm -rd /opt/HDFS/*

h_format: h_hdfs_r
	$(HADOOP_FORMAT)

h_set_input:
	$(HADOOP_EXE) dfs -mkdir input
	$(HADOOP_EXE) dfs -put $(INPUT_DIR)$(INPUT_FILES) input

exec:
	$(RUN)

h_get_output:
	$(HADOOP_EXE) dfs -get output $(OUTPUT_DIR)

run: h_get_output

clean:
	rm -rf $(JAVA_DIR)

clean:
	rm -rf $(JAVA_DIR)
