package message;

import java.io.File;

public class RemoveMsg extends Message{

	public RemoveMsg(String remoteFileName) {
		this.remoteFileName = remoteFileName;
	}
	
	public String getFileName() {
		return this.remoteFileName;
	}
	
	public String getFilePartName() {
		return filePartName;
	}

	public void setFilePartName(String filePartName) {
		this.filePartName = filePartName;
	}

	private static final long serialVersionUID = -226484318375906067L;
	private String remoteFileName = null;
	private String filePartName;
}
