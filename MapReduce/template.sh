#! /bin/bash
template="package example;\n\nimport java.util.Iterator;\nimport java.util.StringTokenizer;\nimport mapreduce.MRBase;\nimport mapreduce.PairContainer;\n\npublic class WordCounter implements MRBase {\n\n\t@Override\n\tpublic void reduce(String key, Iterator<String> values, PairContainer output) {\n\t\t// TODO Auto-generated method stub\n\n\t}\n\n\t@Override\n\tpublic void map(String key, String value, PairContainer output) {\n\t\t// TODO Auto-generated method stub\n\t}\n}\n"
echo -e $template > ./src/example/WordCounter.java
