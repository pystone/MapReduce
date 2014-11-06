/**
 * 
 */
package mapreduce;

import java.io.IOException;
import java.net.Socket;

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
		System.out.println("get a new job: " + job._jobId + " " + job._inFilePath);
	}
}
