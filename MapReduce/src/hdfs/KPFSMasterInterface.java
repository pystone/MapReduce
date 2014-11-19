/**
 * 
 */
package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 * @author PY
 *
 */
public interface KPFSMasterInterface extends Remote {
	public KPFSFileInfo getFileLocation(String relPath) throws RemoteException;
	public boolean addFileLocation(String relPath, int sid, long size) throws RemoteException;
	public void removeFileLocation(String relPath, int sid) throws RemoteException;
}
