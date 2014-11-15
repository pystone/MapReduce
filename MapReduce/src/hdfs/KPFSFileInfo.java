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
	public int _size = 0;
	
	public KPFSFileInfo() {
		
	}
	
	public KPFSFileInfo(String host, int size) {
		_host = host;
		_size = size;
	}
}
