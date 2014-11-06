/**
 * 
 */
package jobcontrol;

import java.awt.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author PY
 *
 */
public class JobManager {
	private static JobManager _sharedJobManager = null;
	public static JobManager sharedJobManager() {
		if (_sharedJobManager == null) {
			_sharedJobManager = new JobManager();
		}
		return _sharedJobManager;
	}
	
	private JobDispatcher _dispatcher = null;
	private Queue<JobInfo> _sendingJobs = null;
	private JobManager() {
		_sendingJobs = new LinkedList<JobInfo>();
		_dispatcher = new JobDispatcher(this);
		_dispatcher.start();
	}
	
	public void sendJob(JobInfo job) {
		_sendingJobs.add(job);
	}
	
	public void sendJobs(Collection<? extends JobInfo> jobs) {
		_sendingJobs.addAll(jobs);
	}
	
	public boolean isSendingQueueEmpty() {
		return _sendingJobs.isEmpty();
	}
	
	public JobInfo getNextJob() {
		return _sendingJobs.poll();
	}
}
