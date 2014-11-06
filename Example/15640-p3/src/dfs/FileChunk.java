/**
 * 
 */
package dfs;

import java.io.Serializable;

/**
 * @author yinxu
 * 
 */
public class FileChunk implements Serializable{

	private static final long serialVersionUID = 7614462656624029368L;
	
	public FileChunk(String localFileFullPath) {
		this.localFileFullPath = localFileFullPath;
	}
	public FileChunk(String localFileFullPath, long startIndex, long endIndex, int partNum) {
		this.localFileFullPath = localFileFullPath;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.partNum = partNum;
	}

	public long startIndex;
	public long endIndex;
	public String localFileFullPath;
	public int partNum;
}
