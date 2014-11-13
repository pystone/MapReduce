#!bin/sh
mkdir tmp
cp ./src/example/*.java ./tmp/
cd tmp
mkdir mapreduce
cd ..
cp ./src/mapreduce/MRBase.java ./tmp/mapreduce/
cp ./src/mapreduce/Pair.java ./tmp/mapreduce/
cp ./src/mapreduce/PairContainer.java ./tmp/mapreduce/
javac ./tmp/mapreduce/*.java
javac ./tmp/*.java
jar -cvf emr.jar ./tmp/*.class