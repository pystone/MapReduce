/**
 * 
 */
package mapreduce;

import hdfs.KPFSException;
import hdfs.KPFSStub;
import hdfs.KPFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
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
	public KPFSStub _kpfs;
	
	private Slave() {
		_kpfs = new KPFSStub();
		
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
		
		// download files
		try {
			job._inputFile = _kpfs.getFile(job._inputFile._relDir, job._inputFile._fileName);
			job._outputFile = _kpfs.getFile(job._outputFile._relDir, job._outputFile._fileName);
			job._mrFile = _kpfs.getFile(job._mrFile._relDir, job._mrFile._fileName);
		} catch (IOException e) {
			System.out.println("Failed to download files!");
			e.printStackTrace();
		}
		
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
		
		String content;
		try {
			content = job._inputFile.getString();
			ins.map(job._inputFile._fileName, content, interPairs);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		job.saveInterFile(interPairs);

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
		
		job.saveResultFile(resultPairs);
		
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
