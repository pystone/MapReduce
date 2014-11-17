package jobcontrol;

import hdfs.KPFile;

import java.util.HashMap;

public class Task {
	public enum TaskPhase{
		NONE,
		MAP,
		REDUCE
	}
	public String _taskName;
	public KPFile _mrFile;
	public HashMap<Integer, JobInfo> _jobs = new HashMap<Integer, JobInfo>();
	public TaskPhase _phase = TaskPhase.NONE;
	
	public Task(String taskName) {
		_taskName = taskName;
	}	
	
	public boolean phaseComplete() {
		if (_phase == TaskPhase.MAP) {
			for (Integer jobId: _jobs.keySet()) {
				JobInfo job = _jobs.get(jobId);
				if (job._type != JobInfo.JobType.MAP_COMPLETE) {
					return false;
				}
			}
			return true;
		}
		if (_phase == TaskPhase.REDUCE) {
			for (Integer jobId: _jobs.keySet()) {
				JobInfo job = _jobs.get(jobId);
				if (job._type != JobInfo.JobType.REDUCE_COMPLETE) {
					return false;
				}
			}
			return true;
		}
		System.out.println(_taskName + " is not a task in map or reduce phase!");
		return false;
	}
}
