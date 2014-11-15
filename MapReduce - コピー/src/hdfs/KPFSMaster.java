/**
 * 
 */
package hdfs;

import java.io.FileNotFoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

/**
 * @author PY
 * 
 */
public class KPFSMaster implements KPFSMasterInterface {

	private HashMap<String, ArrayList<KPFSFileInfo>> _mapTbl = new HashMap<String, ArrayList<KPFSFileInfo>>();

	@Override
	public ArrayList<String> splitFile(String filePath, int chunkSizeB,
			String directory, String fileName) {
		// split file and dispatch them into other nodes
		// return the id of that file (the relative path of those files, like
		// "taskName/InputFiles/taskName.part001")
		try {
			KPFileSplit.split(filePath, chunkSizeB, directory, fileName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public KPFSFileInfo getFileLocation(String relPath) {
		ArrayList<KPFSFileInfo> ips = (ArrayList<KPFSFileInfo>) _mapTbl
				.get(relPath);
		if (ips == null || ips.isEmpty()) {
			return null;
		}

		Random rand = new Random();
		int idx = rand.nextInt(ips.size()); // load balancer entry point

		return ips.get(idx);
	}

	@Override
	public boolean addFileLocation(String relPath, String addr, int size) {
		ArrayList<KPFSFileInfo> ips = _mapTbl.get(relPath);
		if (ips == null) {
			ips = new ArrayList<KPFSFileInfo>();
			_mapTbl.put(relPath, ips);
		}
		KPFSFileInfo val = new KPFSFileInfo(addr, size);
		ips.add(val);
		return true;
	}

	@Override
	public void removeFileLocation(String relPath, String addr) {
		ArrayList<KPFSFileInfo> ips = _mapTbl.get(relPath);
		if (ips == null) {
			return;
		}
		Iterator<KPFSFileInfo> iter = ips.iterator();
		if (iter.hasNext()) {
			KPFSFileInfo info = iter.next();
			if (info._host.equals(addr)) {
				ips.remove(info);
			}
		}
	}

	public static void start(int port) {
		try {
			KPFSMaster obj = new KPFSMaster();
			KPFSMasterInterface stub = (KPFSMasterInterface) UnicastRemoteObject
					.exportObject(obj, 0);

			// Bind the remote object's stub in the registry
			Registry registry = null;
			try {
				registry = LocateRegistry.getRegistry(port);// use any no. less
															// than 55000
				registry.list();
				// This call will throw an exception if the registry does not
				// already exist
			} catch (RemoteException e) {
				registry = LocateRegistry.createRegistry(1099);
			}
			registry.bind("KPFSMasterInterface", stub);

			System.out.println("KPFS master ready");
		} catch (Exception e) {
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}
}
