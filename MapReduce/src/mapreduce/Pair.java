package mapreduce;
/**
 * 
 */


import java.io.Serializable;



/**
 * @author arturh
 * @see http://stackoverflow.com/questions/156275/what-is-the-equivalent-of-the-c-pairl-r-in-java
 *
 */
public class Pair implements Comparable, Serializable {	
    private String first;
    private String second;
    
    public Pair(String key, String val) {
    	super();
    	this.first = key;
    	this.second = val;
    }

    public int hashCode() {
    	int hashFirst = first != null ? first.hashCode() : 0;
    	int hashSecond = second != null ? second.hashCode() : 0;

    	return (hashFirst + hashSecond) * hashSecond + hashFirst;
    }

    public boolean equals(Object other) {
    	if (other instanceof Pair) {
    		Pair otherPair = (Pair) other;
    		return 
    		((  this.first == otherPair.first ||
    			( this.first != null && otherPair.first != null &&
    			  this.first.equals(otherPair.first))) &&
    		 (	this.second == otherPair.second ||
    			( this.second != null && otherPair.second != null &&
    			  this.second.equals(otherPair.second))) );
    	}

    	return false;
    }

    public String toString()
    { 
           return "(" + first + ", " + second + ")"; 
    }

    public String getFirst() {
    	return first;
    }

    public void setFirst(String first) {
    	this.first = first;
    }

    public String getSecond() {
    	return second;
    }

    public void setSecond(String second) {
    	this.second = second;
    }

	@Override
	public int compareTo(Object o) {
		Pair target = (Pair)o;
        return getFirst().compareTo(target.getFirst());
	}
}
