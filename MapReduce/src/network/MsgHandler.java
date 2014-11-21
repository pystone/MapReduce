/**
 * 
 */
package network;

import java.io.IOException;
import java.net.Socket;

import jobcontrol.JobInfo;
import mapreduce.Master;
import mapreduce.Slave;
import mapreduce.SlaveTracker;

/**
 * Used by master.
 * Handle the incoming connection from slaves.
 * A new instance in an independent thread for every connection.
 * 
 * @author PY
 *
 */
public class MsgHandler extends Thread {
	private int _sid;
	private Socket _socket;
	private NetworkFailInterface _failDele;
    
    public MsgHandler(int sid, Socket socket, NetworkFailInterface dele) {
    	_sid = sid;
        _socket = socket;
        _failDele = dele;
    }
        
    public void run() {
    	boolean connAlive = true;
    	while (connAlive) {
    		try {
    			Message msg = null;
    			synchronized(_socket) {
					msg = NetworkHelper.receive(_socket);
    			}
    			
    			if (msg == null) {
					continue;
				}
				switch (msg._type) {
				/* master -> slave */
				case NEW_JOB:
					Slave.sharedSlave().newJob((JobInfo)msg._content);
					break;
					
				/* slave -> master */
				case SLAVE_HEARTBEAT:
					Master.sharedMaster().slaveHeartbeat(msg._source, (SlaveTracker)msg._content);
					break;
				case MAP_COMPLETE:
					Master.sharedMaster().checkMapCompleted((JobInfo)msg._content);
					break;
				case REDUCE_COMPLETE:
					Master.sharedMaster().checkReduceCompleted((JobInfo)msg._content);
					break;
				}
    			
			} catch (ClassNotFoundException | IOException e) {
				connAlive = false;
//				System.out.println("one conn down " + _sid);
//				e.printStackTrace();
			}
    	}
    	_failDele.networkFail(_sid);
    }
}
