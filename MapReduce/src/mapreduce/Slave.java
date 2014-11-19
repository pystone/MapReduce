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
import java.util.Iterator;

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
	}

	public void newJob(JobInfo job) throws RemoteException {
		System.out.println("get a new job: " + job._jobId + " " + job._taskName
				+ " " + job._type);

		// do map or reduce
		if (job._type == JobInfo.JobType.MAP) {
			map(job);
		} else if (job._type == JobInfo.JobType.REDUCE) {
			reduce(job);
		} else {
			System.out.println("WARNING: Receiving a NONE job!");
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
		finishJob(job, Message.MessageType.MAP_COMPLETE);
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
		finishJob(job, Message.MessageType.REDUCE_COMPLETE);
	}

	public void finishJob(JobInfo job, Message.MessageType type) {
		Message fin = new Message();
		fin._type = type;
		fin._source = GlobalInfo.sharedInfo()._sid;
		fin._content = job;
		try {
			NetworkHelper.send(_socket, fin);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
