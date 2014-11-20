/**
 * 
 */
package mapreduce;

import hdfs.KPFSSlave;
import hdfs.KPFSSlaveInterface;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

import jobcontrol.JobInfo;
import network.Message;
import network.MsgHandler;
import network.NetworkFailInterface;
import network.NetworkHelper;

/**
 * @author PY
 * 
 */
public class Slave implements NetworkFailInterface {
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
			ObjectOutputStream out = new ObjectOutputStream(_socket.getOutputStream());
			out.writeObject(GlobalInfo.sharedInfo()._sid);
			
		} catch (IOException e) {
			System.out.println("Connection failed!");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void start(int sid) {
		GlobalInfo.sharedInfo()._sid = sid;
		
		MsgHandler handler = new MsgHandler(sid, _socket, this);
		Thread t = new Thread(handler);
		t.start();
		
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
		

		/* sending heart beat to master */
		while (true) {
			if (!_ready) {
				continue;
			}
			
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			SlaveTracker slvTracker = new SlaveTracker(GlobalInfo.sharedInfo()._sid);
			synchronized (_waitingJob) {
				slvTracker._queueingCnt = _waitingJob.size();
			}
			synchronized (_workingJob) {
				slvTracker._workingCnt = _workingJob.size();
			}
			
			Message heartbeat = new Message(GlobalInfo.sharedInfo()._sid, Message.MessageType.SLAVE_HEARTBEAT);
			heartbeat._content = slvTracker;
			
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
		
//		updateJobInfo(job, Message.MessageType.JOB_UPDATE);
		
		synchronized (_waitingJob) {
			_waitingJob.add(job);
		}
		
	}
	
	public synchronized void updateJobInfo(JobInfo job, Message.MessageType type) {
		if (!(type == Message.MessageType.MAP_COMPLETE || type == Message.MessageType.REDUCE_COMPLETE)) {
			return;
		}
		Message msg = new Message(GlobalInfo.sharedInfo()._sid, type);
		msg._content = job;
		synchronized (job) {
			try {
				NetworkHelper.send(_socket, msg);
			} catch (IOException e) {
				System.err.println("Connection broken. Please restart this slave!");
				System.exit(-1);
			}
		}
		
		if (job._type==JobInfo.JobType.MAP_COMPLETE || job._type==JobInfo.JobType.REDUCE_COMPLETE) {
			synchronized(_workingJob) {
				_workingJob.remove(job);
			}
			
			System.out.println("finish job: " + job._jobId + " " + job._taskName
					+ " " + job._type);
		}
	}

	@Override
	public void networkFail(int sid) {
		// TODO Auto-generated method stub
		
	}
}
