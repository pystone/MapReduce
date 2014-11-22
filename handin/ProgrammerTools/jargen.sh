#! /bin/bash

JAVAFILES=./JavaFiles/*.java
TMPDIR=./tmp/
TMPJAR=jar/
PACK=mapreduce/
JAR=./JarFiles/


mkdir -p $TMPDIR
cp ../MapReduce/mapreduce/MRBase.java $TMPDIR
cp ../MapReduce/mapreduce/Pair.java $TMPDIR
cp ../MapReduce/mapreduce/PairContainer.java $TMPDIR
javac -cp \* $TMPDIR*.java
mkdir $TMPDIR$TMPJAR
mkdir $TMPDIR$TMPJAR$PACK
cp $TMPDIR*.class $TMPDIR$TMPJAR$PACK

for f in $JAVAFILES
do
  echo "Processing $f file..."
  cp $f $TMPDIR
  filename=$(basename $f)
  base=${filename%.*}

  javac -cp \* $TMPDIR*.java
  cp $TMPDIR*.class $TMPDIR$TMPJAR

  jar -cvf $JAR$base.jar $TMPDIR$TMPJAR
  
  rm -rf $TMPDIR$filename
done

rm -rf tmp


