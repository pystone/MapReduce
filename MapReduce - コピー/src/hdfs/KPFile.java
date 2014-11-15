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
public class KPFile {
	public String _relDir = null;	// "taskName/*Files/"
	public String _fileName = null;	// "taskName.part001"
	
	
	public String getFileString() {
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
	
	public byte[] getFileBytes() {
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
			registry = LocateRegistry.getRegistry(GlobalInfo.sharedInfo().DataMasterHost, GlobalInfo.sharedInfo().DataMasterPort);
			masterService = (KPFSMasterInterface) registry.lookup("KPFSMasterInterface");
		} catch (RemoteException | NotBoundException e) {
			System.out.println("Error occurs when looking up service in data master!");
			e.printStackTrace();
		} 
		return masterService;
	}
	
	private KPFSSlaveInterface getSlaveService(String host) {
		Registry registry = null;
        KPFSSlaveInterface slaveService = null;
        try {
			registry = LocateRegistry.getRegistry(host, GlobalInfo.sharedInfo().DataSlavePort);
			slaveService = (KPFSSlaveInterface) registry.lookup("KPFSSlaveInterface");
		} catch (RemoteException | NotBoundException e) {
			System.out.println("Error occurs when looking up service in data node!");
			e.printStackTrace();
		}
        return slaveService;
	}
	
//	private File _file = null;
//	private Scanner _scan = null;
//	
//	
//	public void open() throws FileNotFoundException, KPFSException {
//		if (_relDir == null || _fileName == null) {
//			System.out.println("The path or file name is not set!");
//			throw new KPFSException("The path or file name is not set!");
//		} else {
//			_file = new File(getAbsPath()); 
//			_scan = new Scanner(_file);
//		}
//	}
//	
	public String getRelPath() {
		return _relDir + "/" + _fileName;
	}
//	
	public String getLocalAbsPath() {
		return GlobalInfo.sharedInfo().FileRootDir + getRelPath();
	}
//	
//	public byte[] getByte() throws IOException {
//		byte[] byteArr = new byte[(int)_file.length()];
//		FileInputStream fin = new FileInputStream(_file);
//		BufferedInputStream bin = new BufferedInputStream(fin);
//		bin.read(byteArr, 0, byteArr.length);
//		bin.close();
//		fin.close();
//		return byteArr;
//	}
//	
//	public String getString() throws FileNotFoundException {
//		Scanner exp = new Scanner(_file);
//		String str = "";
//		while (exp.hasNextLine()) {
//			str += exp.nextLine() + '\n';
//		}
//		exp.close();
//		return str;
//	}
//	
	public void saveFileLocally(byte[] byteArr, String localHost) throws IOException {
		File file = new File(getLocalAbsPath());
		file.getParentFile().mkdirs();
		FileOutputStream outStream = new FileOutputStream(file);
		outStream.write(byteArr);
		outStream.close();
		
		KPFSMasterInterface masterService = getMasterService();
		masterService.addFileLocation(getRelPath(), localHost, (int)file.length());
	}
//	
}
