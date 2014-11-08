package mapreduce;

import java.util.Iterator;

public interface Reducer <InKeyT, InValT, OutKeyT, OutValT> {
	
	public void reduce(InKeyT key, Iterator<InValT> values, PairContainer<OutKeyT, OutValT> output) throws Exception;

}