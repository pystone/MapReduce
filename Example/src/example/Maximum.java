package example;

import java.util.Iterator;
import java.util.StringTokenizer;

import example.WordCount.Map;
import example.WordCount.Reduce;

import mapreduce.IntWritable;
import mapreduce.LongWritable;
import mapreduce.MapReduceBase;
import mapreduce.MapReduceConf;
import mapreduce.Mapper;
import mapreduce.OutputCollector;
import mapreduce.Reducer;
import mapreduce.Reporter;
import mapreduce.Text;

public class Maximum {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {
		private final static Text max = new Text("max");
		private LongWritable number = new LongWritable();

		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output,
				Reporter reporter) throws Exception {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				try {
					number.set(tokenizer.nextToken());
				} catch (Exception e) {

				}
				output.collect(max, number);
			}
		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter) throws Exception {
			long maxValue = Long.MIN_VALUE;
			while (values.hasNext()) {
				maxValue = Math.max(maxValue, values.next().get());
			}
			output.collect(key, new LongWritable(maxValue));
		}
	}

	public static MapReduceConf getMapReduceConf() {
		MapReduceConf conf = new MapReduceConf();

		conf.setMapClass(Map.class);
		conf.setReduceClass(Reduce.class);

		conf.setMapInputKeyClass(LongWritable.class);
		conf.setMapInputValueClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(LongWritable.class);

		conf.setReduceInputKeyClass(Text.class);
		conf.setReduceInputValueClass(LongWritable.class);
		conf.setReduceOutputKeyClass(Text.class);
		conf.setReduceOutputValueClass(LongWritable.class);

		return conf;
	}

}
