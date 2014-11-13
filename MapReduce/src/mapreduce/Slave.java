/**
 * 
 */
package mapreduce;

import hdfs.KPFSException;
import hdfs.KPFSSlave;
import hdfs.KPFSSlaveInterface;
import hdfs.KPFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

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
//	public KPFSStub _kpfs;
	
	private Slave() {
//		_kpfs = new KPFSStub();
		
		try {
			_socket = new Socket(GlobalInfo.sharedInfo().MasterHost, GlobalInfo.sharedInfo().MasterPort);
			MsgHandler handler = new MsgHandler(_socket);
            Thread t = new Thread(handler);
            t.start();
		} catch (IOException e) {
			System.out.println("Connection failed!");
			System.exit(-1);
		}
	}
	
	public void start() {
		/* start HDFS */
		try {
            KPFSSlave obj = new KPFSSlave();
            KPFSSlaveInterface stub = (KPFSSlaveInterface) UnicastRemoteObject.exportObject(obj, 0);

            _registry = null;
            try {
            	_registry = LocateRegistry.getRegistry(GlobalInfo.sharedInfo().DataSlavePort);
            	_registry.list();
            }
            catch (RemoteException e) { 
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
	
	public void newJob(JobInfo job) {
		System.out.println("get a new job: " + job._jobId + " " + job._taskName + " " + job._type);
		
		// do map or reduce
		if (job._type == JobInfo.JobType.MAP) {
			map(job);
		} else if (job._type == JobInfo.JobType.REDUCE) {
			reduce(job);
		} else {
			System.out.println("WARNING: Receiving a NONE job!");
		}
	}
	
	public void map(JobInfo job) {
		PairContainer interPairs = new PairContainer();
		MRBase ins = job.getMRInstance();
		
		String content = job._inputFile.getFileString();
		ins.map(job._inputFile._fileName, content, interPairs);
		
		String localhost = "";
		try {
			localhost = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			System.err.println("Network error!");
			e.printStackTrace();
		}
		job.saveInterFile(interPairs, localhost);

		// send complete msg back to master
		finishJob(job);
	}
	
	public void reduce(JobInfo job) {
		PairContainer resultPairs = new PairContainer();
		MRBase ins = job.getMRInstance();
		PairContainer interPairs = job.getInterPairs();
		Iterator<Pair> iter = interPairs.getInitialIterator();
		
		for (; iter.hasNext(); ) {
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
		finishJob(job);
	}
	
	public void finishJob(JobInfo job) {
		Message fin = new Message();
		fin._type = Message.MessageType.JOB_COMPLETE;
		fin._source = _sid;
		fin._content = job;
		try {
			NetworkHelper.send(_socket, fin);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
