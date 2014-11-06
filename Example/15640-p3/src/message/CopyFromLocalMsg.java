package message;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;

import dfs.FileChunk;

public class CopyFromLocalMsg extends Message {

	public CopyFromLocalMsg(ArrayList<String> localFileList,
			ArrayList<Long> localFileSize, InetAddress IP, int port) {
		this.localFileList = localFileList;
		this.localFilesSize = localFileSize;
		this.fileTransferIP = IP;
		this.fileTransferPort = port;
	}

	public InetAddress getFileTransferIP() {
		return this.fileTransferIP;
	}

	public int getFileTransferPort() {
		return this.fileTransferPort;
	}

	public ArrayList<String> getLocalFileListFullPath() {
		return this.localFileList;
	}
	
	public ArrayList<Long> getLocalFileSize() {
		return this.localFilesSize;
	}

	public String getFileName(String fileFullPath) {
		File file = new File(fileFullPath);
		return file.getName();
	}

	public FileChunk getFileChunk() {
		return fileChunk;
	}

	public void setFileChunk(FileChunk fileChunk) {
		this.fileChunk = fileChunk;
	}

	public long getFileLength() {
		return fileLength;
	}

	public void setFileLength(long fileLength) {
		this.fileLength = fileLength;
	}

	private static final long serialVersionUID = -5861057913250168882L;
	private ArrayList<String> localFileList;
	private ArrayList<Long> localFilesSize;
	private InetAddress fileTransferIP;
	private int fileTransferPort;

	private FileChunk fileChunk;
	private long fileLength;

}
