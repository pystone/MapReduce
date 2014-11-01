/**
 * 
 */
package message;

/**
 * @author yinxu
 *
 */
public class NewJobMsg extends Message {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1042777587443317921L;
	
	private String jobName;
	
	public NewJobMsg(String jobName) {
		this.jobName = jobName;
	}

	public String getJobName() {
		return jobName;
	}
	

}
