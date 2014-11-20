/**
 * 
 */
package jobcontrol;

import java.io.IOException;
import java.net.Socket;

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
			synchronized(_sharedManager) {
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
				
				Socket socket = Master.sharedMaster()._slvSocket.get(job._sid);
				if (socket == null) {
					System.out.println(job._taskName + " " + job._jobId + " is down. Ignoring this job.");
					continue;
				}
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
}
