/**
 * 
 */
package mapreduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author PY
 *
 */
public class PairContainer<KeyT, ValT> implements Serializable {
	// TODO: 
	
	public void emit(Pair<KeyT, ValT> pair) {
		
	}
	
	public void emit(KeyT key, ValT val) {
		// TODO
	}
	
	public PairContainer<KeyT, Iterator<ValT>> mergeSameKey() {
		// TODO
		return null;
	}
	
	public Iterator<Pair<String, Iterator<String>>> getInitialIterator() {
		// TODO
		return null;
	}
}
