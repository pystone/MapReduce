JFLAGS = 
JC = javac
DEST = ./bin 
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java -d DEST

CLASSES = \
	./src/example/HelloWorld.java \
	./src/example/TwitterExtractor.java \
	./src/example/WordCounter.java \
	./src/hdfs/KPFile.java \
	./src/hdfs/KPFSException.java \
	./src/hdfs/KPFSFileInfo.java \
	./src/hdfs/KPFSMaster.java \
	./src/hdfs/KPFSMasterInterface.java \
	./src/hdfs/KPFSSlave.java \
	./src/hdfs/KPFSSlaveInterface.java \
	./src/jobcontrol/JobDispatcher.java \
	./src/jobcontrol/JobInfo.java \
	./src/jobcontrol/JobManager.java \
	./src/jobcontrol/Task.java \
	./src/mapreduce/GlobalInfo.java \
	./src/mapreduce/MapReduce.java \
	./src/mapreduce/Master.java \
	./src/mapreduce/MRBase.java \
	./src/mapreduce/Pair.java \
	./src/mapreduce/PairContainer.java \
	./src/mapreduce/Slave.java \
	./src/mapreduce/SlaveTracker.java \
	./src/mapreduce/SlaveWork.java \
	./src/network/Listen.java \
	./src/network/Message.java \
	./src/network/MsgHandler.java \
	./src/network/NetworkFailInterface.java \
	./src/network/NetworkHelper.java

MAIN = ./src/mapreduce/MapReduce
	
default: classes

classes: $(CLASSES:.java=.class)

run:
	java $(MAIN)

clean:	
	find . -name "*.class" -type f -delete
	
