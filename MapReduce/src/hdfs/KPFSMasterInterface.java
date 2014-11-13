/**
 * 
 */
package hdfs;

import java.rmi.Remote;

/**
 * @author PY
 *
 */
public interface KPFSMasterInterface extends Remote {
	public KPFSFileInfo getFileLocation(String relPath);
	public boolean addFileLocation(String relPath, String addr, int size);
	public void removeFileLocation(String relPath, String addr);
}
