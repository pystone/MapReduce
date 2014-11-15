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
	public ArrayList<String> splitFile(String filePath, int chunkSizeB,
			String directory, String fileName) throws RemoteException;
	public KPFSFileInfo getFileLocation(String relPath) throws RemoteException;
	public boolean addFileLocation(String relPath, String addr, long size) throws RemoteException;
	public void removeFileLocation(String relPath, String addr) throws RemoteException;
}
