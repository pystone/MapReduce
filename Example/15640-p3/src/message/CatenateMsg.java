package message;

import java.io.File;

public class CatenateMsg extends Message{
	
	public CatenateMsg(String fileName) {
		this.fileName = fileName;
	}
	
	public String getFileName() {
		return this.fileName;
	}
	
	public String getFilePartName() {
		return filePartName;
	}

	public void setFilePartName(String filePartName) {
		this.filePartName = filePartName;
	}

	public String getCatReply() {
		return catReply;
	}

	public void setCatReply(String catReply) {
		this.catReply = catReply;
	}
	
	private String fileName = null;
	private String filePartName;
	private String catReply = null;
	private static final long serialVersionUID = -722239763841540007L;

}
