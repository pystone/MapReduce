/**
 * 
 */
package mapreduce;

import java.util.ArrayList;

/**
 * @author PY
 *
 */
public abstract class MRBase implements Mapper<String, String, String, String>, 
Reducer<String, String, String, String> {
//	private ArrayList<Class> _types = null;
//	public Map _mapclazz = null;
//	public Reduce _reduceclazz = null;
//	
//	public abstract class Map implements Mapper<String, String, String, String> {}
//	public abstract class Reduce implements Reducer<String, String, String, String> {}
//	
//	public abstract void configType();
//	
//	public MRBase() {
//		 _types = new ArrayList<Class>(4);
//	}
//	
//	public void setInputKeyType(Class cls) {
//		_types.set(0, cls);
//	}
//	public void setInputValType(Class cls) {
//		_types.set(1, cls);
//	}
//	public void setOutputKeyType(Class cls) {
//		_types.set(1, cls);
//	}
//	public void setOutputValType(Class cls) {
//		_types.set(3, cls);
//	}
//	public Class getInputKeyType() {
//		return _types.get(0);
//	}
//	public Class getInputValType() {
//		return _types.get(1);
//	}
//	public Class getOutputKeyType() {
//		return _types.get(2);
//	}
//	public Class getOutputValType() {
//		return _types.get(3);
//	}
}
