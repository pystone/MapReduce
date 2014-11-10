package example;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;


import mapreduce.MRBase;
import mapreduce.PairContainer;

public class WordCounter extends MRBase {
//	public class Map implements Mapper<String, String, String, String> {
//
//		@Override
//		public void map(String key, String value,
//				PairContainer<String, String> output) throws Exception {
//			output.emit(key, "1");
//		}
//		
//	}
//	
//	public class Reduce implements Reducer<String, String, String, String> {
//
//		@Override
//		public void reduce(String key, Iterator<String> values,
//				PairContainer<String, String> output) throws Exception {
//			Integer sum = 0;
//			while (values.hasNext()) {
//				sum += Integer.parseInt(values.next());
//			}
//			output.emit(key, sum.toString());
//		}
//		
//	}
	public static void haha()	 {
		System.out.println("hello world!!");
	}
//	@Override
//	public void configType() {
//		setInputKeyType(String.class);
//		setInputValType(String.class);
//		setOutputKeyType(String.class);
//		setOutputValType(String.class);
//		
//	}
	@Override
	public void reduce(String key, ArrayList<String> values,
			PairContainer output) {
		Integer sum = 0;
		Iterator<String> itor = values.iterator();
		
		while (itor.hasNext()) {
			sum += Integer.parseInt(itor.next());
		}
		output.emit(key, sum.toString());
		
	}
	@Override
	public void map(String key, String value,
			PairContainer output) {
		
		StringTokenizer tokenizer = new StringTokenizer(value);
		while (tokenizer.hasMoreTokens()) {
			output.emit(tokenizer.nextToken(), "1");
			System.out.println(tokenizer.nextToken());
		}
	}
	@Override
	public void reduce(String key, String values, PairContainer output)
			throws Exception {
		// TODO Auto-generated method stub
		
	}
}
