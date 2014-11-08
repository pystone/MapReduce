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
	KeyT key;
	ValT val;
	
	ArrayList<Class> types = new ArrayList<Class>();
	
	public void emit(KeyT key, ValT val) {
		// TODO
	}
	
	public PairContainer<KeyT, Iterator<ValT>> mergeSameKey() {
		// TODO
		return null;
	}
	
	public void getType() {
		
		types.add((Class)((KeyT)(new Object()).getClass()));
		types.add((Class)((ValT)(new Object()).getClass()));
		
		System.out.println(((KeyT)types.get(0)).getClass());
		System.out.println(((ValT)types.get(1)).getClass());
		
//		System.out.println(key.getClass());
		
//		System.out.println(val.getClass().getName());
	}
}
