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
	
	@Override
    public int hashCode() {
		return _sid;
	}
	
	@Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final KPFSFileInfo other = (KPFSFileInfo) obj;
        return _sid==other._sid;
        
    }
}
