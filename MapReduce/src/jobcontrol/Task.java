package jobcontrol;

import java.util.HashMap;

public class Task {
	public String _taskName;
	public int _taskId;
	public HashMap<Integer, JobInfo> _jobs = new HashMap<Integer, JobInfo>();
	public String _mapperPath;
	public String _reducerPath;
	
	public Task(int taskId, String taskName) {
		_taskId = taskId;
		_taskName = taskName;
	}	
}
