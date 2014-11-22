#! /bin/bash

JAVAFILES=./JavaFiles/*.java
TMPDIR=./tmp/
TMPJAR=jar/
PACK=mapreduce/
JAR=./JarFiles/

if [ -d "$TMPDIR" ]; then
  rm -rf $TMPDIR
fi

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
  cp $TMPDIR$base.class $TMPDIR$TMPJAR

  cd $TMPDIR$TMPJAR
  jar -cvf ../../$JAR$base.jar .
  cd ../../
  
  echo "Removing $TMPDIR$filename"
  rm -rf $TMPDIR$filename
  echo "Removing $TMPDIR$TMPJAR$base.class"
  rm -rf "$TMPDIR$TMPJAR$base.class"
done

rm -rf tmp


