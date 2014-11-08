/**
 * 
 */
package mapreduce;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;

import jobcontrol.JobInfo;
import network.MsgHandler;

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
	
	private Slave() {
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
		
	}
	
	public void newJob(JobInfo job) {
		System.out.println("get a new job: " + job._jobId + " " + job._taskName + " " + job._type);
		if (job._type == JobInfo.JobType.MAP) {
			map(job);
		} else if (job._type == JobInfo.JobType.REDUCE) {
			reduce(job);
		} else {
			System.out.println("WARNING: Receiving a NONE job!");
		}
	}
	
	public void map(JobInfo job) {
		PairContainer<String, String> interPairs = new PairContainer<String, String>();
		MRBase ins = job.getMRInstance();
		String inFileName = job.getInFileName();
		String content = job.getFileContent();
		try {
			ins.map(inFileName, content, interPairs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		PairContainer<String, Iterator<String>> mergedInterPairs = interPairs.mergeSameKey();
		job.saveInterFile(mergedInterPairs);
	}
	
	public void reduce(JobInfo job) {
		
	}
}
