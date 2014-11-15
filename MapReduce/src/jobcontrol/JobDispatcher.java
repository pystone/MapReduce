/**
 * 
 */
package jobcontrol;

import java.io.IOException;

import mapreduce.Master;
import network.Message;
import network.NetworkHelper;

/**
 * @author PY
 * 
 */
public class JobDispatcher extends Thread {
	JobManager _sharedManager = null;

	public JobDispatcher(JobManager manager) {
		_sharedManager = manager;
	}

	public void run() {
		while (true) {
			if (_sharedManager.isSendingQueueEmpty() == true) {
				continue;
			}
			JobInfo job = _sharedManager.getNextJob();
			if (job == null) {
				continue;
			}
			Message msg = new Message();
			msg._type = Message.MessageType.NEW_JOB;
			msg._source = 0;
			msg._content = job;
			try {
				NetworkHelper.send(
						Master.sharedMaster()._slvSocket.get(job._sid), msg);
			} catch (IOException e) {
				// TODO: error handle!
				e.printStackTrace();
			}
		}
	}
}
