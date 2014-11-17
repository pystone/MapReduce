package mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author PY
 *
 */
public class PairContainer implements Serializable {
	private static final long serialVersionUID = 4824504881487447089L;
	
	public ArrayList<Pair> _list = new ArrayList<Pair>();

	
	public PairContainer() {
	}
	
	public PairContainer(Iterator<Pair> itor) {
		while(itor.hasNext()) {
			_list.add(itor.next());
		}
	}

	public void emit(Pair pair) {
		_list.add(pair);
	}
	
	public void emit(String key, String val) {
		Pair pair = new Pair(key, val);		
		emit(pair);
	}
	
	public void mergeSameKey() {
		String currentKey = null;
		
		ArrayList<String> list = null;
		ArrayList<Pair> newList = new ArrayList<Pair>();
		
		Collections.sort(_list);
		
		for(Pair pair : _list) {
			String key = pair.getFirst();
			
			if(key.equals(currentKey)) {
				Iterator<String> val = pair.getSecond();
				while(val.hasNext()) {
					list.add(val.next());
				}
			} else {
				if(currentKey != null) {
					Pair newPair = new Pair(currentKey, list.iterator());			
					newList.add(newPair);
				}
				list = new ArrayList<String>();
				Iterator<String> val = pair.getSecond();
				while(val.hasNext()) {
					list.add(val.next());
				}
				currentKey = key;
			}
		}
		if(currentKey != null) {
			Pair newPair = new Pair(currentKey, list.iterator());			
			newList.add(newPair);
		}
		
		Collections.sort(newList);
		_list = newList;
	}
	
	public Iterator<Pair> getInitialIterator() {
		return _list.iterator() ;
	}
	
	// use some special ASCII code as delimiter
//	public void saveResultFile(String path) {
//		(new File(path)).getParentFile().mkdirs();
//		
//		FileOutputStream os = saveResultStream(path);
//		try {
//			os.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	} 
//	
//	public FileOutputStream saveResultStream(String path) {
//		FileOutputStream os = null;
//		BufferedWriter bw = null;
//		try {
//			os = new FileOutputStream(path);
//			bw = new BufferedWriter(new OutputStreamWriter(os));
//			
//			for(Pair pair : _list) {
//				String key = pair.getFirst();
//				Iterator<String> val = pair.getSecond();
//				
//				StringBuilder valStr = new StringBuilder();
//				while(val.hasNext()) {
//					valStr.append(val.next());
//					valStr.append(";");
//				}
//				bw.write(key + "\t" + valStr.toString());
//				bw.newLine();
//			}
//			bw.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		return os;
//	}
	
	// PairContainer => key1:value1,value2,value3;key2:value1,value2,value3;
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Iterator<Pair> itor = _list.iterator();
		int i = 0;
		while(itor.hasNext()) {
			Pair pair = itor.next();
			if(i > 0) {
				sb.append("\n");
			}
			sb.append(pair.toString());
			i++;
		}
		return sb.toString();
	}
	
	// key1:value1,value2,value3;key2:value1,value2,value3; => PairContainer 
	public void restoreFromString(String str) {
		if(str == null) {
			return;
		}
		String[] pairStrs = str.split("\n");
		for(String pairStr : pairStrs) { 
			String[] parts = pairStr.split(":");
			String key = parts[0];
			String valueList = parts[1];
			if(valueList != null) {
				String[] values = parts[1].split(",");
				for(String value : values) {
					Pair pair = new Pair(key, value);
					_list.add(pair);
				}
			}
		}
		mergeSameKey();
	}
}
