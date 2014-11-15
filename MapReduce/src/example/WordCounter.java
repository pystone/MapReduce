package example;

import java.util.Iterator;
import java.util.StringTokenizer;
import mapreduce.MRBase;
import mapreduce.PairContainer;

public class WordCounter implements MRBase {

	@Override
	public void reduce(String key, Iterator<String> values, PairContainer output) {
		// TODO Auto-generated method stub

	}

	@Override
	public void map(String key, String value, PairContainer output) {
		// TODO Auto-generated method stub
	}
}

