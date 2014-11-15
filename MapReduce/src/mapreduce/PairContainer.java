package mapreduce;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * @author PY
 *
 */
public class PairContainer implements Serializable {
	private static final long serialVersionUID = 4824504881487447089L;
	
	private PriorityQueue<Pair> queue = new PriorityQueue<Pair>();
	
	public PairContainer() {
	}
	
	public PairContainer(Iterator<Pair> itor) {
		while(itor.hasNext()) {
			queue.offer(itor.next());
		}
	}

	public void emit(Pair pair) {
		queue.offer(pair);
	}
	
	public void emit(String key, String val) {
		Pair pair = new Pair(key, val);		
		emit(pair);
	}
	
	public void mergeSameKey() {
		String currentKey = null;
		
		ArrayList<String> list = null;
		PriorityQueue<Pair> newQueue = new PriorityQueue<Pair>();
		
		for(Pair pair : queue) {
			String key = pair.getFirst();
			
			if(key.equals(currentKey)) {
				Iterator<String> val = pair.getSecond();
				while(val.hasNext()) {
					list.add(val.next());
				}
			} else {
				if(currentKey != null) {
					Pair newPair = new Pair(currentKey, list.iterator());			
					newQueue.offer(newPair);
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
			newQueue.offer(newPair);
		}
		
		queue = newQueue;
	}
	
	public Iterator<Pair> getInitialIterator() {
		return queue.iterator() ;
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
			
			for(Pair pair : queue) {
				String key = pair.getFirst();
				Iterator<String> val = pair.getSecond();
				
				StringBuilder valStr = new StringBuilder();
				while(val.hasNext()) {
					valStr.append(val.next());
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
	
	// PairContainer => key1:value1,value2,value3;key2:value1,value2,value3;
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Iterator<Pair> itor = queue.iterator();
		int i = 0;
		while(itor.hasNext()) {
			Pair pair = itor.next();
			if(i > 0) {
				sb.append(";");
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
		String[] pairStrs = str.split(";");
		for(String pairStr : pairStrs) { 
			String[] parts = pairStr.split(":");
			String key = parts[0];
			String valueList = parts[1];
			if(valueList != null) {
				String[] values = parts[1].split(",");
				for(String value : values) {
					Pair pair = new Pair(key, value);
					queue.offer(pair);
				}
			}
		}
		mergeSameKey();
	}
}
