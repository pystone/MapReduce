package mapreduce;
/**
 * 
 */


import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * @author PY
 *
 */
public class PairContainer implements Serializable {
	private static final long serialVersionUID = 4824504881487447089L;
	
	public TreeMap<String, ArrayList<String>> map = new TreeMap<String, ArrayList<String>>();

	public void emit(Pair pair) {
		String key = pair.getFirst();
		String val = pair.getSecond();
		emit(key, val);
	}
	
	public void emit(String key, String val) {
		ArrayList<String> list = null;
		
		if(map.containsKey(key)) {
			list = map.get(key);
		} else {
			list = new ArrayList<String>();
		}
		list.add(val);
		map.put(key, list);
	}
	
//	public PairContainer mergeSameKey() {
//		return null;
//	}
	
	public Iterator<Entry<String, ArrayList<String>>> getInitialIterator() {
		return map.entrySet().iterator() ;
	}
	
	// use some special ASCII code as delimiter
	public void saveResultFile(String path) {
		FileOutputStream os = saveResultStream(path);
		try {
			os.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	} 
	
	public FileOutputStream saveResultStream(String path) {
		FileOutputStream os = null;
		BufferedWriter bw = null;
		try {
			os = new FileOutputStream(path);
			bw = new BufferedWriter(new OutputStreamWriter(os));
			
			for(Map.Entry<String, ArrayList<String>> entry : map.entrySet()) {
				String key = entry.getKey();
				ArrayList<String> val = entry.getValue();
				
				StringBuilder valStr = new StringBuilder();
				for(String str : val) {
					valStr.append(str);
					valStr.append(";");
				}
				bw.write(key + "\t" + valStr.toString());
				bw.newLine();
			}
			
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return os;
	}
}
