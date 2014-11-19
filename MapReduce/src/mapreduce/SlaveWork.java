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
				synchronized (Slave.sharedSlave()._workingMap) {
					curWorking += Slave.sharedSlave()._workingMap.size();
				}
				synchronized (Slave.sharedSlave()._workingReduce) {
					curWorking += Slave.sharedSlave()._workingReduce.size();
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
		if (_job._type == JobInfo.JobType.MAP) {
			synchronized (Slave.sharedSlave()._workingMap) {
				Slave.sharedSlave()._workingMap.add(_job);
			}
			
			try {
				map(_job);
			} catch (RemoteException e) {
				System.out.println("ERROR: failed to do map!");
				_job.serialize();
				e.printStackTrace();
			}
		} else {
			synchronized (Slave.sharedSlave()._workingReduce) {
				Slave.sharedSlave()._workingReduce.add(_job);
			}
			
			try {
				reduce(_job);
			} catch (RemoteException e) {
				System.out.println("ERROR: failed to do reduce!");
				_job.serialize();
				e.printStackTrace();
			}
		}
	}
	
	public void map(JobInfo job) throws RemoteException {
		PairContainer interPairs = new PairContainer();
		MRBase ins = job.getMRInstance();

		for (int i=0; i<job._inputFile.size(); ++i) {
			String content = job._inputFile.get(i).getFileString();
			ins.map(job._inputFile.get(i)._fileName, content, interPairs);
		}
		
		interPairs.mergeSameKey();

		job.saveInterFile(interPairs);
		job._type = JobInfo.JobType.MAP_COMPLETE;

		// send complete msg back to master
		Slave.sharedSlave().finishJob(job, Message.MessageType.MAP_COMPLETE);
	}

	public void reduce(JobInfo job) throws RemoteException {
		PairContainer resultPairs = new PairContainer();
		MRBase ins = job.getMRInstance();
		PairContainer interPairs = job.getInterPairs();
		System.out.println(interPairs.toString());
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
		Slave.sharedSlave().finishJob(job, Message.MessageType.REDUCE_COMPLETE);
	}
	
}
