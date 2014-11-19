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
	public ArrayList<JobInfo> _workingMap = new ArrayList<JobInfo>();
	public ArrayList<JobInfo> _workingReduce = new ArrayList<JobInfo>();
	public ArrayList<JobInfo> _waitingJob = new ArrayList<JobInfo>();


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
	}

	public void newJob(JobInfo job) {
		System.out.println("get a new job: " + job._jobId + " " + job._taskName
				+ " " + job._type);

		synchronized (_waitingJob) {
			_waitingJob.add(job);
		}
		
	}

	public synchronized void finishJob(JobInfo job, Message.MessageType type) {
		if (job._type!=JobInfo.JobType.MAP_COMPLETE && job._type!=JobInfo.JobType.REDUCE_COMPLETE) {
			System.out.println("WARNING: try to finish an incompleted job!");
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
		
		if (job._type == JobInfo.JobType.MAP_COMPLETE) {
			synchronized(_workingMap) {
				_workingMap.remove(job);
			}
		} else {
			synchronized(_workingReduce) {
				_workingReduce.remove(job);
			}
		}
		
		System.out.println("Finish " + job._taskName + job._jobId + " on " + GlobalInfo.sharedInfo()._sid);
	}
}
