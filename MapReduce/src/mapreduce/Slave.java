/**
 * 
 */
package mapreduce;

import hdfs.KPFSException;
import hdfs.KPFSMasterInterface;
import hdfs.KPFSSlave;
import hdfs.KPFSSlaveInterface;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.StandardCopyOption;
import java.rmi.NotBoundException;
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

	public int _sid;
	public Socket _socket;
	public Registry _registry;

	// public KPFSStub _kpfs;

	private Slave() {
		// _kpfs = new KPFSStub();

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

	public void start() {
		/* start HDFS */
		try {
			KPFSSlave obj = new KPFSSlave();
			KPFSSlaveInterface stub = (KPFSSlaveInterface) UnicastRemoteObject
					.exportObject(obj, 0);

			_registry = null;
			try {
				_registry = LocateRegistry
						.getRegistry(GlobalInfo.sharedInfo().DataSlavePort);
				_registry.list();
			} catch (RemoteException e) {
				System.err.println("Failed to open HDFS service!");
				e.printStackTrace();
			}
			_registry.bind("KPFSSlaveInterface", stub);

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

		String content = job._inputFile.get(0).getFileString();
		ins.map(job._inputFile.get(0)._fileName, content, interPairs);
		interPairs.mergeSameKey();

		String localhost = "";
		try {
			localhost = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			System.err.println("Network error!");
			e.printStackTrace();
		}
		job.saveInterFile(interPairs, localhost);

		// send complete msg back to master
		finishJob(job, Message.MessageType.MAP_COMPLETE);
	}

	public void reduce(JobInfo job) throws RemoteException {
		PairContainer resultPairs = new PairContainer();
		MRBase ins = job.getMRInstance();
		PairContainer interPairs = job.getInterPairs();
		Iterator<Pair> iter = interPairs.getInitialIterator();

		for (; iter.hasNext();) {
			Pair cur = iter.next();
			try {
				ins.reduce(cur.getFirst(), cur.getSecond(), resultPairs);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		String localhost = "";
		try {
			localhost = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			System.err.println("Network error!");
			e.printStackTrace();
		}
		job.saveResultFile(resultPairs, localhost);

		// send complete msg back to master
		finishJob(job, Message.MessageType.REDUCE_COMPLETE);
	}

	public void finishJob(JobInfo job, Message.MessageType type) {
		Message fin = new Message();
		fin._type = type;
		fin._source = _sid;
		fin._content = job;
		try {
			NetworkHelper.send(_socket, fin);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
