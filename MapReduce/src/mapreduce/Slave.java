/**
 * 
 */
package mapreduce;

import hdfs.KPFSException;
import hdfs.KPFile;

import java.io.FileNotFoundException;
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
	
	public void newJob(Object[] content) {
		JobInfo job = (JobInfo) content[0];
		
		/* save the file into disk */
		String fileContent = (String) content[1];
		KPFile file = new KPFile(true);
		try {
			file.restoreFromString(job.getInputFileName(), fileContent);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
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
		KPFile file = new KPFile(true);
		try {
			file.open(inFileName);
			String content = file.exportToString();
			ins.map(inFileName, content, interPairs);
		} catch (FileNotFoundException | KPFSException e1) {
			e1.printStackTrace();
		}
		
		PairContainer<String, Iterator<String>> mergedInterPairs = interPairs.mergeSameKey();

		job.saveInterFile(mergedInterPairs);
		// TODO: send complete msg back to master
	}
	
	public void reduce(JobInfo job) {
		PairContainer<String, String> resultPairs = new PairContainer<String, String>();
		MRBase ins = job.getMRInstance();
		PairContainer<String, Iterator<String>> interPairs = job.getInterPairs();
		Iterator<Pair<String, Iterator<String>>> iter = interPairs.getInitialIterator();
		
		for (; iter.hasNext(); ) {
			Pair<String, Iterator<String>> cur = iter.next();
			try {
				ins.reduce(cur.getFirst(), cur.getSecond(), resultPairs);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		job.saveResultFile(resultPairs);
		// TODO: send complete msg back to master
	}
}
