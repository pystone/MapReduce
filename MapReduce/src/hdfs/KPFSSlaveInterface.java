/**
 * 
 */
package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @author PY
 *
 */
public interface KPFSSlaveInterface extends Remote {
	public String getFileString(String relPath) throws RemoteException, KPFSException;
	public byte[] getFileBytes(String relPath) throws RemoteException, KPFSException;
	public void storeFile(String relPath, byte[] content) throws RemoteException, KPFSException;
}
