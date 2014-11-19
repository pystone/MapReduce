/**
 * 
 */
package hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;

import network.NetworkHelper;
import mapreduce.GlobalInfo;

/**
 * @author PY
 * 
 */
public class KPFile implements Serializable {

	private static final long serialVersionUID = 225432063820226038L;
	public String _relDir = null; // "taskName/*Files/"
	public String _fileName = null; // "taskName.part001"

	public KPFile(String relDir, String fileName) {
		_relDir = relDir;
		_fileName = fileName;
	}

	public String getFileString() throws RemoteException {
		/* retrieve location information from data master */
		KPFSMasterInterface masterService = NetworkHelper.getMasterService();
		KPFSFileInfo info = masterService.getFileLocation(getRelPath());
		
		if (info == null) {
			throw new RemoteException("Cannot find the location of the file: " + getRelPath());
		}

		/* retrieve file content from actual data node */
		KPFSSlaveInterface slaveService = NetworkHelper.getSlaveService(info._sid);
		String content = null;
		try {
			content = slaveService.getFileString(getRelPath());
		} catch (KPFSException e) {
			System.out.println("Failed to read content from KPFile!");
			e.printStackTrace();
		}

		return content;
	}

	public byte[] getFileBytes() throws RemoteException {
		KPFSMasterInterface masterService = NetworkHelper.getMasterService();
		KPFSFileInfo info = masterService.getFileLocation(getRelPath());

		/* retrieve file content from actual data node */
		KPFSSlaveInterface slaveService = NetworkHelper.getSlaveService(info._sid);
		byte[] content = null;
		try {
			content = slaveService.getFileBytes(getRelPath());
		} catch (KPFSException e) {
			System.out.println("Failed to read content from KPFile!");
			e.printStackTrace();
		}

		return content;
	}

	public String getRelPath() {
		return _relDir + _fileName;
	}

	public String getLocalAbsPath() {
		return GlobalInfo.sharedInfo().getLocalRootDir() + getRelPath();
	}

	public void saveFileLocally(byte[] byteArr)
			throws IOException {
		File file = new File(getLocalAbsPath());
		file.getParentFile().mkdirs();

		FileOutputStream outStream = new FileOutputStream(file, false);
		outStream.write(byteArr);
		outStream.write("\n".getBytes());
		outStream.close();
		
		/* save the metadata of this file to master */
		KPFSMasterInterface masterService = NetworkHelper.getMasterService();
		masterService.addFileLocation(getRelPath(), GlobalInfo.sharedInfo()._sid,
				(int) file.length());
		
	}
}
