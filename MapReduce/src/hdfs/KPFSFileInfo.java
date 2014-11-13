/**
 * 
 */
package hdfs;

/**
 * @author PY
 *
 */
public class KPFSFileInfo {
	public String _host = "";
	public int _size = 0;
	
	public KPFSFileInfo() {
		
	}
	
	public KPFSFileInfo(String host, int size) {
		_host = host;
		_size = size;
	}
}
