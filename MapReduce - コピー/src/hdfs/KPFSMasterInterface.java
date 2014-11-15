/**
 * 
 */
package hdfs;

import java.rmi.Remote;
import java.util.ArrayList;

/**
 * @author PY
 * 
 */
public interface KPFSMasterInterface extends Remote {
	public ArrayList<String> splitFile(String filePath, int chunkSizeB,
			String directory, String fileName);

	public KPFSFileInfo getFileLocation(String relPath);

	public boolean addFileLocation(String relPath, String addr, int size);

	public void removeFileLocation(String relPath, String addr);

}
