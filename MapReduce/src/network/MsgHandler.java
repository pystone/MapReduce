/**
 * 
 */
package network;

import java.io.IOException;
import java.net.Socket;

import jobcontrol.JobInfo;
import mapreduce.GlobalInfo;
import mapreduce.Master;
import mapreduce.Slave;

/**
 * Used by master.
 * Handle the incoming connection from slaves.
 * A new instance in an independent thread for every connection.
 * 
 * @author PY
 *
 */
public class MsgHandler extends Thread {
	private Socket _socket;
    
    public MsgHandler(Socket socket) {
        _socket = socket;
    }
        
    public void run() {
    	boolean connAlive = true;
    	while (connAlive) {
    		try {
    			synchronized(_socket) {
					Message msg = NetworkHelper.receive(_socket);
					if (msg == null) {
						continue;
					}
					switch (msg._type) {
					/* master -> slave */
					case NEW_JOB:
						Slave.sharedSlave().newJob((JobInfo)msg._content);
						break;
						
					/* slave -> master */
					case HELLO_SID:
						Master.sharedMaster().newSlave(_socket, msg._source);
						break;
					case MAP_COMPLETE:
						Master.sharedMaster().checkMapCompleted((JobInfo)msg._content);
						break;
					case REDUCE_COMPLETE:
						Master.sharedMaster().checkReduceCompleted((JobInfo)msg._content);
						break;
					}
    			}
			} catch (ClassNotFoundException | IOException e) {
				connAlive = false;
			}
    	}
    }
}
