/**
 * 
 */
package jobcontrol;

import java.io.Serializable;

/**
 * @author PY
 *
 */
public class JobInfo implements Serializable {
	private static final long serialVersionUID = 5710312452396530832L;

	public enum JobType {
		NONE,
		MAP,
		REDUCE
	};
	public int _jobId = 0;
	public int _taskId = 0;
	public int _sid = 0;
	public JobType _type = JobInfo.JobType.NONE;
	public String _inFilePath = "";
	public String _interFileDir = "";
	public String _outFileDir = "";
	public String _mapperPath = "";
	public String _reducerPath = "";
	
	public JobInfo(int jobId) {
		_jobId = jobId;
	}
}
