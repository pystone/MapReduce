#! /bin/bash
mkdir -p tmp
cp ./src/example/*.java ./tmp
cp ./src/mapreduce/MRBase.java ./tmp/
cp ./src/mapreduce/Pair.java ./tmp/
cp ./src/mapreduce/PairContainer.java ./tmp/
javac -cp \* ./tmp/*.java
cd tmp 
rm *.java
jar -cvf example.jar .
mv example.jar ../
cd ..
rm -rf tmp

