package mapreduce;

import java.io.Serializable;

public class Text implements Serializable, Cloneable{

	private static final long serialVersionUID = 2778453252714387988L;
	private String value = "";
	
	public Text() {
		
	}
	
	public Text(String string) {
		// TODO Auto-generated constructor stub
		this.value = string;
	}

	public void set(String value) {
		// TODO Auto-generated method stub
		this.value = value;
	}
	
	public Text clone() {
		Text clone = new Text();
		clone.set(this.value);
		return clone;
	}
	
	public String toString() {
		return this.value;
	}
	
	public int getHashcode() {
		return this.value.hashCode();
	}

}
