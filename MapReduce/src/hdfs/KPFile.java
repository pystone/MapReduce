/**
 * 
 */
package hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Scanner;

import mapreduce.GlobalInfo;
import network.Message;

/**
 * @author PY
 * 
 */
public class KPFile implements Serializable {
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

		/* retrieve file content from actual data node */
		KPFSSlaveInterface slaveService = getSlaveService(info._host);
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
		KPFSSlaveInterface slaveService = getSlaveService(info._host);
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
					.lookup("KPFSMasterInterface");
		} catch (RemoteException | NotBoundException e) {
			System.out
					.println("Error occurs when looking up service in data master!");
			e.printStackTrace();
		}
		return masterService;
	}

	private KPFSSlaveInterface getSlaveService(String host) {
		Registry registry = null;
		KPFSSlaveInterface slaveService = null;
		try {
			registry = LocateRegistry.getRegistry(host,
					GlobalInfo.sharedInfo().DataSlavePort);
			slaveService = (KPFSSlaveInterface) registry
					.lookup("KPFSSlaveInterface");
		} catch (RemoteException | NotBoundException e) {
			System.out
					.println("Error occurs when looking up service in data node!");
			e.printStackTrace();
		}
		return slaveService;
	}

	public String getRelPath() {
		return _relDir + "/" + _fileName;
	}

	public String getLocalAbsPath() {
//		return GlobalInfo.sharedInfo().getLocalRootDir() + getRelPath();
		return getRelPath();
	}

	public void saveFileLocally(byte[] byteArr, String localHost)
			throws IOException {
		File file = new File(getLocalAbsPath());
		file.getParentFile().mkdirs();

		FileOutputStream outStream = new FileOutputStream(file, true);
		outStream.write(byteArr);
		outStream.write("\n".getBytes());
		outStream.close();

		KPFSMasterInterface masterService = getMasterService();
		masterService.addFileLocation(getRelPath(), localHost,
				file.length());
	}
	//
}
