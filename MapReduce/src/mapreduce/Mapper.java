package mapreduce;

public interface Mapper<InKeyT, InValT, OutKeyT, OutValT> {

	public void map(InKeyT key, InValT value, PairContainer output);
}