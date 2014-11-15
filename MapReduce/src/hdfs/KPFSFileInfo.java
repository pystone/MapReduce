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
	public String _host = "";
	public long _size = 0;
	
	public KPFSFileInfo() {
		
	}
	
	public KPFSFileInfo(String host, long size) {
		_host = host;
		_size = size;
	}
}
