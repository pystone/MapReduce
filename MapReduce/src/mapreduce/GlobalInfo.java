/**
 * 
 */
package mapreduce;

/**
 * @author PY
 *
 */
public class GlobalInfo {
	
	public int FileChunkSizeMB = 10;
	public int FileChunkSizeB = 27;
	public int MasterPort = 7888;
	public int SlavePort = 8999;
	public String MasterHost = "ymac";
	public String IntermediateDirName = "IntermediateFiles";
	public String ChunkDirName = "ChunkInputFiles";
	public String ResultDirName = "ResultFiles";
	public String SlaveRootDir = "/tmp/mapreduce/";
	public String FileRootDir = "/tmp/mapreduce/";
	
	
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
}
