package mapreduce;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;



/**
 * @author arturh
 * @see http://stackoverflow.com/questions/156275/what-is-the-equivalent-of-the-c-pairl-r-in-java
 *
 */
public class Pair implements Comparable, Serializable {	
    private String first;
    private ArrayList<String> list;
    
    public Pair(String line) {
    	if(line == null) {
    		return;
    	}
    	String[] parts = line.trim().split(":");
    	this.first = parts[0];
    	if(list == null) {
    		list = new ArrayList<String>();
    	}
		list.add(parts[1]); 
    }
    
    public Pair(String key, String val) {
    	this.first = key;
    	if(list == null) {
    		list = new ArrayList<String>();
    	}
		list.add(val);
    }
    
    public Pair(String key, Iterator<String> val) {
    	this.first = key;
    	
    	while(val.hasNext()) {
	    	if(list == null) {
	    		list = new ArrayList<String>();
	    	}
			list.add(val.next());
    	}
    }
    
//    public int hashCode() {
//    	int hashFirst = first != null ? first.hashCode() : 0;
//    	
//    	int hashSecond = 0;
//    	for(String str : list) {
//    		hashSecond += str.hashCode();
//    	}
//
//    	return (hashFirst + hashSecond);
//    }

//    public boolean equals(Object other) {
//    	if (other instanceof Pair) {
//    		Pair otherPair = (Pair) other;
//    		return 
//    		((  this.first == otherPair.first ||
//    			( this.first != null && otherPair.first != null &&
//    			  this.first.equals(otherPair.first))) &&
//    		 (	this.list == otherPair.list ||
//    			( this.list != null && otherPair.list != null &&
//    			  this.list.equals(otherPair.list))) );
//    	}
//
//    	return false;
//    }

    public String toString()
    { 
    	StringBuilder sb = new StringBuilder();
    	for(int i = 0; i < list.size(); i++) {
    		if(i > 0) {
    			sb.append(",");
    		}
			sb.append(list.get(i));
    	}
    	
        return first + ":" + sb.toString(); 
    }

    public String getFirst() {
    	return first;
    }

    public void setFirst(String first) {
    	this.first = first;
    }

    public Iterator<String> getSecond() {
    	return list.iterator();
    }

    public void setSecond(Iterator<String> second) {
    	while(second.hasNext()) {
    		list.add(second.next());
    	}
    }

	@Override
	public int compareTo(Object other) {
		Pair target = (Pair)other;
		if(getFirst().compareTo(target.getFirst()) > 0) {
			return 1;
		} else if(getFirst().compareTo(target.getFirst()) < 0) {
			return -1;
		} else {
			return 0;
		}
	}
}
