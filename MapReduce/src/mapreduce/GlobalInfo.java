/**
 * 
 */
package mapreduce;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
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
	public String MasterHost = "128.237.162.33";
	public HashMap<Integer, String> SID2Host = new HashMap<Integer, String>();
	
	// For KPFS
	public int FileChunkSizeB = 27;
	public int NumberOfReducer = 3;
	
	public String MasterRootDir = "/tmp/mapreduce/";
	
	public String IntermediateDirName = "IntermediateFiles";
	public String ChunkDirName = "ChunkInputFiles";
	public String ResultDirName = "ResultFiles";
	
	public String JarFilePath = "jar";
	public String JarFileName = "WordCounter.jar";
	
	public String DataMasterHost = "128.237.162.33";
	public int DataMasterPort = 9987;
	public int DataSlavePort = 9986;
	
	public HashMap<String, String> Host2RootDir = new HashMap<String, String>(); 
	
	
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
		return SID2Host.get(sid);
	}
	public String getRootDirByHost(String host) {
		return Host2RootDir.get(host);
	}
	
	public String getLocalRootDir() {
		String localhost = getLocalHost();
		return getRootDirByHost(localhost);
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
		Set<String> shosts = Host2RootDir.keySet();
		for (String slave: shosts) {
			if (shosts.equals(host)) {
				return true;
			}
		}
		return false;
	}
	
}
