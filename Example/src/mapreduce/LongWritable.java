package mapreduce;

import java.io.Serializable;

public class LongWritable implements Serializable{

	private static final long serialVersionUID = 531330805588485283L;
	private long value = 0;

	
	public LongWritable() {
		
	}
	
	public LongWritable(long l) {
		this.value = l;
	}

	public void set(String nextToken) {
		// TODO Auto-generated method stub
		this.value = Long.parseLong(nextToken);
	}
	 
	public void set(long l) {
		this.value = l;
	}

	public long get() {
		// TODO Auto-generated method stub
		return this.value;
	}
	
	public String toString () {
		return "" + this.value;
	}
	
	public LongWritable clone() {
		LongWritable clone = new LongWritable();
		clone.set(this.value);
		return clone;
	}

}
