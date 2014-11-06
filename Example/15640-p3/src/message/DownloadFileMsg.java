/**
 * 
 */
package message;

import java.net.InetAddress;

import mapreduce.MapReduceTask;

/**
 * @author remonx
 *
 */
public class DownloadFileMsg extends Message{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7833509890771396091L;
	
	private String fileFullPath;
	private boolean isSuccessful;
	private int jobId;
	private MapReduceTask task;
	
	public DownloadFileMsg(InetAddress desIP, int desPort, int jobId) {
		this.desIP = desIP;
		this.desPort = desPort;
		this.jobId = jobId;
	}

	public String getFileFullPath() {
		return fileFullPath;
	}

	public void setFileFullPath(String fileFullPath) {
		this.fileFullPath = fileFullPath;
	}

	public boolean isSuccessful() {
		return isSuccessful;
	}

	public void setSuccessful(boolean isSuccessful) {
		this.isSuccessful = isSuccessful;
	}

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public MapReduceTask getTask() {
		return task;
	}

	public void setTask(MapReduceTask task) {
		this.task = task;
	}
	
	

}
