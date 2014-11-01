package mapreduce;


public interface Mapper<InputKey, InputValue, OutputKey, OutputValue> {

	public void map(InputKey key, InputValue value, OutputCollector<OutputKey, OutputValue> output, Reporter reporter) throws Exception;
}
