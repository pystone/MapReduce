/**
 * 
 */
package hdfs;

import java.rmi.Remote;

/**
 * @author PY
 *
 */
public interface KPFSSlaveInterface extends Remote {
	public String getFileString(String relPath) throws KPFSException;
	public byte[] getFileBytes(String relPath) throws KPFSException;
	public void storeFile(String relPath, byte[] content) throws KPFSException;
}
