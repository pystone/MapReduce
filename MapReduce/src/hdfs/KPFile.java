/**
 * 
 */
package hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

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
		KPFSMasterInterface masterService = getMasterService();
		KPFSFileInfo info = masterService.getFileLocation(getRelPath());
		
		if (info == null) {
			throw new RemoteException("Cannot find the location of the file: " + getRelPath());
		}

		/* retrieve file content from actual data node */
		KPFSSlaveInterface slaveService = getSlaveService(info._sid);
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
		KPFSMasterInterface masterService = getMasterService();
		KPFSFileInfo info = masterService.getFileLocation(getRelPath());

		/* retrieve file content from actual data node */
		KPFSSlaveInterface slaveService = getSlaveService(info._sid);
		byte[] content = null;
		try {
			content = slaveService.getFileBytes(getRelPath());
		} catch (KPFSException e) {
			System.out.println("Failed to read content from KPFile!");
			e.printStackTrace();
		}

		return content;
	}

	private KPFSMasterInterface getMasterService() {
		Registry registry = null;
		KPFSMasterInterface masterService = null;
		try {
			registry = LocateRegistry.getRegistry(
					GlobalInfo.sharedInfo().DataMasterHost,
					GlobalInfo.sharedInfo().DataMasterPort);
			masterService = (KPFSMasterInterface) registry
					.lookup("DataMaster");
		} catch (RemoteException | NotBoundException e) {
			System.out
					.println("Error occurs when looking up service in data master!");
			e.printStackTrace();
		}
		return masterService;
	}

	private KPFSSlaveInterface getSlaveService(int sid) {
		Registry registry = null;
		KPFSSlaveInterface slaveService = null;
		try {
			registry = LocateRegistry.getRegistry(GlobalInfo.sharedInfo().getSlaveHostBySID(sid),
					GlobalInfo.sharedInfo().getDataSlavePort(sid));
			slaveService = (KPFSSlaveInterface) registry
					.lookup("DataSlave");
		} catch (RemoteException | NotBoundException e) {
			System.out
					.println("Error occurs when looking up service in data node!");
			e.printStackTrace();
		}
		return slaveService;
	}

	public String getRelPath() {
		return _relDir + _fileName;
	}

	public String getLocalAbsPath() {
		return GlobalInfo.sharedInfo().getLocalRootDir() + getRelPath();
//		return getRelPath();
	}

	public void saveFileLocally(byte[] byteArr)
			throws IOException {
		File file = new File(getLocalAbsPath());
		file.getParentFile().mkdirs();

		FileOutputStream outStream = new FileOutputStream(file, true);
		outStream.write(byteArr);
		outStream.write("\n".getBytes());
		outStream.close();
		
		saveFileMetadataToMaster((int) file.length());
	}
	
	public void saveFileMetadataToMaster(int size) throws RemoteException {
		KPFSMasterInterface masterService = getMasterService();
		masterService.addFileLocation(getRelPath(), GlobalInfo.sharedInfo()._sid,
				size);
	}
}
