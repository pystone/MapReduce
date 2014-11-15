package jobcontrol;

import java.util.HashMap;

public class Task {
	public int _taskId;
	public HashMap<Integer, JobInfo> _jobs = new HashMap<Integer, JobInfo>();
	public String _mapperPath;
	public String _reducerPath;
	
	public Task(int taskId) {
		_taskId = taskId;
	}
	
}
