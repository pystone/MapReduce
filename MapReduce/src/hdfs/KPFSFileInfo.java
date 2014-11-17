/**
 * 
 */
package hdfs;

import java.io.Serializable;

/**
 * @author PY
 *
 */
public class KPFSFileInfo implements Serializable {
	public long _size = 0;
	public int _sid = -1;
	
	public KPFSFileInfo() {
		
	}
	
	public KPFSFileInfo(int sid, long size) {
		_sid = sid;
		_size = size;
	}
}
