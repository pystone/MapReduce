/**
 * 
 */
package mapreduce;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

/**
 * @author PY
 *
 */
public class GlobalInfo {
	// For map-reduce
	public int MasterPort = 7888;
	public int SlavePort = 8999;
	public String MasterHost = "73.52.255.101";
	public HashMap<Integer, String> SID2Host = new HashMap<Integer, String>();
	
	// For KPFS
	public int FileChunkSizeB = 27;
	public int NumberOfReducer = 3;
	
	public String MasterRootDir = "/tmp/master";
	
	public String IntermediateDirName = "IntermediateFiles";
	public String ChunkDirName = "ChunkInputFiles";
	public String ResultDirName = "ResultFiles";
	public String UserDirName = "UserFiles";
	
//	public String JarFilePath = "jar";
//	public String JarFileName = "WordCounter.jar";
	
	public String DataMasterHost = "73.52.255.101";
	public int DataMasterPort = 9980;
	public int DataSlavePort = 9990;
	
//	public HashMap<String, String> Host2RootDir = new HashMap<String, String>();
	public HashMap<Integer, String> Host2RootDir = new HashMap<Integer, String>();
	
	/* set by every node */
	public int _sid = -1;
	
	
	public static GlobalInfo _sharedInfo = null;
	public static GlobalInfo sharedInfo() {
		if (_sharedInfo == null) {
			_sharedInfo = new GlobalInfo();
		}
		return _sharedInfo;
	}
	
	/* Make constructor private to ensure its singleton */
	private GlobalInfo() {
		
	}
	
	public String getSlaveHostBySID(int sid) {
		String ret = SID2Host.get(sid);
		if(ret == null) {
			return "";
		}
		return ret;
	}
	public String getRootDirByHost(String host) {
		String ret = Host2RootDir.get(host);
		if(ret == null) {
			return "";
		}
		return ret;
	}
	
//	public String getLocalRootDir() {
//		String localhost = getLocalHost();
//		return getRootDirByHost(localhost);
//	}
	
	public String getLocalRootDir() {
		return Host2RootDir.get(_sid);
	}
	
	public String getLocalHost() {
		String localhost = "";
		try {
			localhost = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			System.err.println("Network error!");
			e.printStackTrace();
		}
		return localhost;
	}
	
	public boolean isMaster() {
		String host = getLocalHost();
		return host.equals(MasterHost);
	}
	
	public boolean isSlave() {
		String host = getLocalHost();
		Collection<String> shosts = Host2RootDir.values();
		for (String slave: shosts) {
			if (shosts.equals(host)) {
				return true;
			}
		}
		return false;
	}
	
	public Integer[] getActiveSlaveId() {
		Set<Integer> tmpKey = ((HashMap<Integer, String>)SID2Host.clone()).keySet();
		tmpKey.remove(0);
		return (Integer[]) tmpKey.toArray();
	}
	
	public int getDataSlavePort(int sid) {
		return DataSlavePort + sid;
	}
}
