// package example

import java.util.Iterator;
import java.util.StringTokenizer;

import mapreduce.MRBase;
import mapreduce.PairContainer;

public class WordCounter implements MRBase {

	@Override
	public void reduce(String key, Iterator<String> values,
			PairContainer output) {
		System.out.println("This is reduce method." + key);
//		Integer sum = 0;
//		while (values.hasNext()) {
//			sum += Integer.parseInt(values.next());
//		}
//		output.emit(key, sum.toString());
		
	}
	@Override
	public void map(String key, String value,
			PairContainer output) {
		System.out.println("This is map method??? " + key);
		
//		StringTokenizer tokenizer = new StringTokenizer(value);
//		while (tokenizer.hasMoreTokens()) {
//			output.emit(tokenizer.nextToken(), "1");
//			System.out.println(tokenizer.nextToken());
//		}
	}


	
}
