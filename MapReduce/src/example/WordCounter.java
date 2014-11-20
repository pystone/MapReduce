package example;


import java.util.Iterator;
import java.util.Scanner;

import mapreduce.MRBase;
import mapreduce.PairContainer;

public class WordCounter implements MRBase {

	@Override
	public void reduce(String key, Iterator<String> values,
			PairContainer output) {
		Integer sum = 0;
		while (values.hasNext()) {
			sum += Integer.parseInt(values.next());
		}
		output.emit(key, sum.toString());
		
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public void map(String key, String value,
			PairContainer output) {
		
		Scanner scan = new Scanner(value);
		scan.useDelimiter("\n");
		while(scan.hasNext()) {
			output.emit(scan.next(), "1");
		}
		
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
