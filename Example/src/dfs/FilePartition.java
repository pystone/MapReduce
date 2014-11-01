/**
 * 
 */
package dfs;

import java.util.ArrayList;

/**
 * @author yinxu
 * generate a list of file chunks that are represent as range
 *
 */
public class FilePartition {
	
	public FilePartition(String localFileFullPath, long fileLength) {
		this.localFileFullPath = localFileFullPath;
		this.fileLength = fileLength;
		this.chunkSize = YZFS.NUM_RECORDS; /* # of records per chunk */
	}
	
	public ArrayList<FileChunk> generateFileChunks() {
		ArrayList<FileChunk> list = new ArrayList<FileChunk>();
		int numOfRecords = (int) (fileLength / YZFS.RECORD_LENGTH);
		int partNum = 0;
		for(int i = 0; i < numOfRecords; i += chunkSize) {
			FileChunk chunk = new FileChunk(localFileFullPath);
			if (i + chunkSize < numOfRecords) {
				/* full size chunk */
				chunk.startIndex = i * YZFS.RECORD_LENGTH;
				chunk.endIndex = (i + chunkSize) * YZFS.RECORD_LENGTH - 1;
			} else {
				/* partial size chunk */
				chunk.startIndex = i * YZFS.RECORD_LENGTH;;
				chunk.endIndex = fileLength - 1;
			}
			chunk.partNum = partNum++;
			list.add(chunk);
		}
		
		return list;
	}
	
	private String localFileFullPath;
	private long fileLength;
	private int chunkSize;
}
