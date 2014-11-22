#! /bin/bash
if [ "$1" != "" ]; then
    template="import java.util.Iterator;\nimport java.util.StringTokenizer;\nimport mapreduce.MRBase;\nimport mapreduce.PairContainer;\n\npublic class $1 implements MRBase {\n\n\t@Override\n\tpublic void reduce(String key, Iterator<String> values, PairContainer output) {\n\t\t// TODO Please write your reduce function here.\n\n\t}\n\n\t@Override\n\tpublic void map(String key, String value, PairContainer output) {\n\t\t// TODO Please write your map function here.\n\t}\n}\n"
    echo -e $template > ./JavaFiles/$1.java
else
    echo "Please provide task name! Note: only task name is needed and do not add '.java'. "
fi



