/**
 * 
 */
package example;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import mapreduce.*;
import mapreduce.OutputCollector.Entry;

public class WordCount {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws Exception {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws Exception {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static MapReduceConf getMapReduceConf() {
		MapReduceConf conf = new MapReduceConf();

		conf.setMapClass(Map.class);
		conf.setReduceClass(Reduce.class);
		
		conf.setMapInputKeyClass(LongWritable.class);
		conf.setMapInputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setReduceInputKeyClass(Text.class);
		conf.setReduceInputValueClass(IntWritable.class);
		conf.setReduceOutputKeyClass(Text.class);
		conf.setReduceOutputValueClass(IntWritable.class);

		return conf;
	}

	public static void mapper(String input) throws Throwable {

		// should know the type of output key and output value from user
		OutputCollector mapOutput = new OutputCollector();
		OutputCollector combineOutput = new OutputCollector();

		// dummy reporter, no use
		Reporter reporter = new Reporter();
		WordCount.Map mapper = new WordCount.Map();
		WordCount.Reduce combiner = new WordCount.Reduce();

		// input value is one line from the input file
		Text inputValue = new Text();
		BufferedReader bufferedReader = new BufferedReader(new FileReader(input));
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			inputValue.set(line);
			mapper.map(null, inputValue, mapOutput, reporter);
		}

		// while (mapOutput.queue.size() != 0)
		// System.out.print(mapOutput.queue.poll() + " ");

		System.out.println("debug");

		// start combine
		mapreduce.OutputCollector.Entry entry = (Entry) mapOutput.queue.poll();
		mapreduce.OutputCollector.Entry tmpEntry = null;
		ArrayList<IntWritable> values = new ArrayList<IntWritable>();
		Iterator<IntWritable> itrValues = null;

		Text key = (Text) entry.getKey();
		values.add((IntWritable) entry.getValue());

		Method method = key.getClass().getMethod("getHashcode", null);
		int hash = ((Integer) method.invoke(key, null));
		int tmpHash = 0;
		while (mapOutput.queue.size() != 0) {
			tmpEntry = (Entry) mapOutput.queue.poll();
			tmpHash = ((Integer) method.invoke(tmpEntry.getKey(), null));
			if (tmpHash == hash) {
				values.add((IntWritable) tmpEntry.getValue());
			} else {
				itrValues = values.iterator();
				combiner.reduce(key, itrValues, combineOutput, reporter);
				entry = tmpEntry;
				key = (Text) entry.getKey();
				hash = ((Integer) method.invoke(key, null));
				values = new ArrayList<IntWritable>();
				values.add((IntWritable) entry.getValue());
			}
		}

		// don't forget the last one :)
		itrValues = values.iterator();
		combiner.reduce(key, itrValues, combineOutput, reporter);

		System.out.println("debug");

		FileOutputStream fileOut = new FileOutputStream(input + ".out");
		ObjectOutputStream objOut = new ObjectOutputStream(fileOut);
		objOut.writeObject(combineOutput);

		// while (combineOutput.queue.size() != 0)
		// System.out.print(combineOutput.queue.poll() + "  ");
	}

	@SuppressWarnings({ "rawtypes", "unused", "resource" })
	public static void reducer(String[] inputs) throws Throwable {

		WordCount.Reduce reducer = new WordCount.Reduce();
		Reporter reporter = new Reporter();
		OutputCollector reduceOutput = new OutputCollector();

		int size = inputs.length;
		OutputCollector[] reduceInputs = new OutputCollector[size];
		Entry[] entries = new Entry[size];

		for (int i = 0; i < size; i++) {
			FileInputStream fileIn = new FileInputStream(inputs[i]);
			ObjectInputStream objIn = new ObjectInputStream(fileIn);
			reduceInputs[i] = ((OutputCollector) objIn.readObject());
			entries[i] = (Entry) reduceInputs[i].queue.poll();
		}

		Object key = entries[0].getKey();
		Method method = key.getClass().getMethod("getHashcode", null);
		ArrayList<Integer> minIndices = null;

		while ((minIndices = getMinIndices(entries, method)) != null) {
			key = entries[minIndices.get(0)].getKey();
			ArrayList values = new ArrayList();
			Iterator itrValues = null;

			for (int i : minIndices) {
				values.add(entries[i].getValue());
				entries[i] = (Entry) reduceInputs[i].queue.poll();
			}

			itrValues = values.iterator();
			reducer.reduce((Text) key, itrValues, reduceOutput, reporter);
		}

		// while (reduceOutput.queue.size() != 0)
		// System.out.print(reduceOutput.queue.poll() + "  ");

	}

	public static ArrayList<Integer> getMinIndices(Entry[] entries, Method method)
			throws Throwable, NoSuchMethodException {
		ArrayList<Integer> ret = null;
		int length = entries.length;
		int minHash = Integer.MAX_VALUE;

		for (int i = 0; i < length; i++) {
			if (entries[i] == null)
				continue;

			int hash = ((Integer) method.invoke(entries[i].getKey(), null));
			if (hash < minHash) {
				minHash = hash;
				ret = new ArrayList<Integer>();
				ret.add(i);
			} else if (hash == minHash) {
				ret.add(i);
			}
		}

		return ret;
	}

	@SuppressWarnings({ "rawtypes", "resource", "unchecked" })
	public static void main(String[] args) throws Throwable {

		// mapper("test1.txt");
		// mapper("test2.txt");
		// mapper("test3.txt");
		// reducer(new String[] { "test1.txt.out", "test2.txt.out",
		// "test3.txt.out" });

		// JobConf conf = new JobConf(WordCount.class);
		// conf.setJobName("wordcount");
		//
		// conf.setOutputKeyClass(Text.class);
		// conf.setOutputValueClass(IntWritable.class);
		//
		// conf.setMapperClass(Map.class);
		// conf.setCombinerClass(Reduce.class);
		// conf.setReducerClass(Reduce.class);
		//
		// conf.setInputFormat(TextInputFormat.class);
		// conf.setOutputFormat(TextOutputFormat.class);
		//
		// FileInputFormat.setInputPaths(conf, new Path(args[0]));
		// FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		//
		// JobClient.runJob(conf);
	}
}