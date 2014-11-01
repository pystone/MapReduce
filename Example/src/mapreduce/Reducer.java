package mapreduce;

import java.util.Iterator;

public interface Reducer <InputKey, InputValue, OutputKey, OutputValue> {
	
	public void reduce(InputKey key, Iterator<InputValue> values, OutputCollector<OutputKey, OutputValue> output, Reporter reporter) throws Exception;

}
