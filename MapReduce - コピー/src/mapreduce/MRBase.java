/**
 * 
 */
package mapreduce;

import java.util.Iterator;

/**
 * @author PY
 *
 */

public interface MRBase {
	public void map(String key, String value, PairContainer output);
	public void reduce(String key, Iterator<String> values, PairContainer output);
}

