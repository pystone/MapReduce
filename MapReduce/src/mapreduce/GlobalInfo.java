/**
 * 
 */
package mapreduce;

/**
 * @author PY
 *
 */
public class GlobalInfo {
	
	int FileChunkSizeMB = 10;
	int FileChunkSizeB = 27;
	int MasterPort = 7888;
	int SlavePort = 8999;
	String MasterHost = "ymac";
	String IntermediateDirName = "IntermediateFiles";
	String ChunkDirName = "ChunkInputFiles";
	String ResultDirName = "ResultFiles";
	
	
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
