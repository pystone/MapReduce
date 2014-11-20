/**
 * 
 */
package mapreduce;

import java.rmi.RemoteException;
import java.util.Iterator;

import jobcontrol.JobInfo;
import network.Message;

/**
 * @author PY
 *
 */
public class SlaveWork extends Thread {
	private JobInfo _job;
	private boolean _isWorker;
	public SlaveWork(JobInfo job, boolean isWorker) {
		_job = job;
		_isWorker = isWorker;
	}
	
	public void run() {
		if (_isWorker) {
			work();
		} else {
			/* periodically check if there are waiting jobs and put them in job if possible */
			while (true) {
				JobInfo newJob = null;
				synchronized (Slave.sharedSlave()._waitingJob) {
					if (Slave.sharedSlave()._waitingJob.isEmpty() == false) {
						newJob = Slave.sharedSlave()._waitingJob.get(0);
					}
				}
				
				if (newJob == null) {
					continue;
				}
				
				int curWorking = 0;
				synchronized (Slave.sharedSlave()._workingJob) {
					curWorking += Slave.sharedSlave()._workingJob.size();
				}
				
				if (curWorking > GlobalInfo.sharedInfo().SID2Capacity.get(GlobalInfo.sharedInfo()._sid)) {
					newJob = null;
				}
				
				if (newJob != null) {
					synchronized (Slave.sharedSlave()._waitingJob) {
						Slave.sharedSlave()._waitingJob.remove(newJob);
					}
					SlaveWork newWork = new SlaveWork(newJob, true);
					newWork.start();
				}
			}
		}
		
	}
	
	private void work() {
		System.out.println("start a new job: " + _job._jobId + " " + _job._taskName
		+ " " + _job._type);
		
		synchronized (Slave.sharedSlave()._workingJob) {
			Slave.sharedSlave()._workingJob.add(_job);
		}
		if (_job._type == JobInfo.JobType.MAP_QUEUE) {
			
			try {
				map(_job);
			} catch (RemoteException e) {
				System.out.println("ERROR: failed to do map!");
				_job.serialize();
				e.printStackTrace();
			}
		} else if (_job._type == JobInfo.JobType.REDUCE_QUEUE) {
			
			try {
				reduce(_job);
			} catch (RemoteException e) {
				System.out.println("ERROR: failed to do reduce!");
				_job.serialize();
				e.printStackTrace();
			}
		} else {
			System.out.println("WARNING: try to begin a job that is not in queue phase! (" + _job._type + ")");
			return;
		}
	}
	
	public boolean map(JobInfo job) throws RemoteException {
		synchronized (job) {
			job._type = JobInfo.JobType.MAP;
		}
//		Slave.sharedSlave().updateJobInfo(job, Message.MessageType.JOB_UPDATE);
		
		PairContainer interPairs = new PairContainer();
		MRBase ins = job.getMRInstance();

		for (int i=0; i<job._inputFile.size(); ++i) {
			String content = job._inputFile.get(i).getFileString();
			if (content == null) {
				System.out.println("Failed to get file " + job._inputFile.get(i).getRelPath() + ". Aborting this job.");
				return false;
			}
			ins.map(job._inputFile.get(i)._fileName, content, interPairs);
		}
		
		interPairs.mergeSameKey();

		job.saveInterFile(interPairs);
		job._type = JobInfo.JobType.MAP_COMPLETE;

		// send complete msg back to master
		Slave.sharedSlave().updateJobInfo(job, Message.MessageType.MAP_COMPLETE);
		
		return true;
	}

	public boolean reduce(JobInfo job) throws RemoteException {
		synchronized (job) {
			job._type = JobInfo.JobType.REDUCE;
		}
//		Slave.sharedSlave().updateJobInfo(job, Message.MessageType.JOB_UPDATE);
		
		PairContainer resultPairs = new PairContainer();
		MRBase ins = job.getMRInstance();
		PairContainer interPairs = job.getInterPairs();
		if (interPairs == null) {
			System.out.println("Failed to read inter files. Aborting this job.");
			return false;
		}
		Iterator<Pair> iter = interPairs.getInitialIterator();

		while(iter.hasNext()) {
			Pair pair = iter.next();
			String key = pair.getFirst();
			Iterator<String> second = pair.getSecond();
			
			try {
				ins.reduce(key, second, resultPairs);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		job.saveResultFile(resultPairs);
		job._type = JobInfo.JobType.REDUCE_COMPLETE;

		// send complete msg back to master
		Slave.sharedSlave().updateJobInfo(job, Message.MessageType.REDUCE_COMPLETE);
		
		return true;
	}
	
}
