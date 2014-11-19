/**
 * 
 */
package mapreduce;

import hdfs.KPFSSlave;
import hdfs.KPFSSlaveInterface;

import java.io.IOException;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

import jobcontrol.JobInfo;
import network.Message;
import network.MsgHandler;
import network.NetworkHelper;

/**
 * @author PY
 * 
 */
public class Slave {
	private static Slave _sharedSlave;

	public static Slave sharedSlave() {
		if (_sharedSlave == null) {
			_sharedSlave = new Slave();
		}
		return _sharedSlave;
	}

	public Socket _socket;
	public ArrayList<JobInfo> _waitingJob = new ArrayList<JobInfo>();
	public ArrayList<JobInfo> _workingJob = new ArrayList<JobInfo>();
	private boolean _ready = false;

	private Slave() {

		try {
			_socket = new Socket(GlobalInfo.sharedInfo().MasterHost,
					GlobalInfo.sharedInfo().MasterPort);
			MsgHandler handler = new MsgHandler(_socket);
			Thread t = new Thread(handler);
			t.start();
		} catch (IOException e) {
			System.out.println("Connection failed!");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void start(int sid) {
		GlobalInfo.sharedInfo()._sid = sid;
		
		/* tell master my sid */
		Message msg = new Message();
		msg._type = Message.MessageType.HELLO_SID;
		msg._source = sid;
		try {
			NetworkHelper.send(_socket, msg);
		} catch (IOException e1) {
			System.err.println("Network problem. Failed to tell master my sid " + sid);
			e1.printStackTrace();
			System.exit(-1);
		}
		
		/* start HDFS */
		try {
			KPFSSlave obj = new KPFSSlave();
			KPFSSlaveInterface stub = (KPFSSlaveInterface) UnicastRemoteObject
					.exportObject(obj, 0);

			Registry _registry = null;
			try {
				_registry = LocateRegistry
						.getRegistry(GlobalInfo.sharedInfo().getDataSlavePort(GlobalInfo.sharedInfo()._sid));
				_registry.list();
			} catch (RemoteException e) {
				_registry = LocateRegistry.createRegistry(GlobalInfo.sharedInfo().getDataSlavePort(GlobalInfo.sharedInfo()._sid));
			}
				
			_registry.bind("DataSlave", stub);

			System.out.println("HDFS ready");
		} catch (Exception e) {
			System.err.println("Failed to open HDFS service!");
			e.printStackTrace();
		}
		
		/* start the coordinator of workers */
		SlaveWork work = new SlaveWork(null, false);
		work.start();
		
		while (true) {
			if (!_ready) {
				continue;
			}
			
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			Object[] tracker = {_waitingJob.toArray(), _workingJob.toArray()};
			Message heartbeat = new Message(GlobalInfo.sharedInfo()._sid, Message.MessageType.SLAVE_HEARTBEAT);
			heartbeat._content = tracker;
			try {
				NetworkHelper.send(_socket, heartbeat);
			} catch (IOException e) {
				System.err.println("Connection broken. Please restart this slave!");
				System.exit(-1);
			}
		}
	}
	
	public void slaveReady() {
		_ready = true;
	}

	public void newJob(JobInfo job) {
//		System.out.println("get a new job: " + job._jobId + " " + job._taskName
//				+ " " + job._type);

		/* update phase */
		if (job._type == JobInfo.JobType.MAP_READY) {
			job._type = JobInfo.JobType.MAP_QUEUE;
		} else if (job._type == JobInfo.JobType.REDUCE_READY) {
			job._type = JobInfo.JobType.REDUCE_QUEUE;
		} else {
			System.out.println("WARNING: try to queue a job that is not in ready phase! (" + job._type + ")");
			return;
		}
		
		synchronized (_waitingJob) {
			_waitingJob.add(job);
		}
		
	}

	public synchronized void finishJob(JobInfo job, Message.MessageType type) {
		if (job._type!=JobInfo.JobType.MAP_COMPLETE && job._type!=JobInfo.JobType.REDUCE_COMPLETE) {
			System.out.println("WARNING: try to finish an incompleted job! (" + job._type + ")");
			return;
		}
		
		Message fin = new Message();
		fin._type = type;
		fin._source = GlobalInfo.sharedInfo()._sid;
		fin._content = job;
		
		try {
			NetworkHelper.send(_socket, fin);
		} catch (IOException e) {
			System.err.println("Broken pipe! Please restart this slave program!");
			System.exit(-1);
		}
		
		synchronized(_workingJob) {
			_workingJob.remove(job);
		}
		
		System.out.println("finish job: " + job._jobId + " " + job._taskName
				+ " " + job._type);
	}
}
