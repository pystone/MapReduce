/**
 * 
 */
package jobcontrol;

import java.util.HashMap;

/**
 * @author PY
 *
 */
public class UnfinishedJobs {
	public class ValPair extends HashMap<Integer, Integer> {
		public ValPair(int key, int val) {
			super();
			this.put(key, val);
		}
		public ValPair() {
			super();
		}
	}
	
	private HashMap<String, ValPair> _jobs = new HashMap<String, ValPair>();
	
	public void addNewJob(String taskName, int jobId, int sid) {
		ValPair pair = _jobs.get(jobId);
		if (pair == null) {
			pair = new ValPair();
			_jobs.put(taskName, pair);
		}
		pair.put(jobId, sid);
	}
	
	public void removeJob(String taskName, int jobId, int sid) {
		ValPair pair = _jobs.get(jobId);
		if (pair == null || !pair.containsKey(jobId)) {
			System.out.println("WARNING: removing a non-existing task " + taskName + " " + jobId);
			return;
		}
		
		if (pair.remove(jobId, sid) == false) {
			System.out.println("WARNING: the existing sid (" + pair.get(jobId) + ") does not match the input sid (" + sid + ").");
		}
		
		if (pair.isEmpty()) {
			_jobs.remove(taskName);
		}
	}
	
	public boolean hasUnfinishedJobs(String taskName) {
		ValPair pair = _jobs.get(taskName);
		if (pair == null || pair.isEmpty()) {
			return true;
		}
		return false;
	}
}
