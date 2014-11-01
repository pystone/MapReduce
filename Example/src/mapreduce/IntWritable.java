package mapreduce;

import java.io.Serializable;

public class IntWritable implements Serializable{
	
	private static final long serialVersionUID = -4484555373125100200L;
	private int value;

	public IntWritable() {
		
	}
	
	public IntWritable(int i) {
		this.value = i;
	}

	public int get() {
		// TODO Auto-generated method stub
		return this.value;
	}
	
	public String toString () {
		return "" + this.value;
	}
	
	public IntWritable clone() {
		IntWritable clone = new IntWritable(this.value);
		return clone;
	}

}
