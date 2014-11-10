/**
 * 
 */
package network;

import java.io.IOException;
import java.net.Socket;

import jobcontrol.JobInfo;
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
				Message msg = NetworkHelper.receive(_socket);
				if (msg == null) {
					continue;
				}
				switch (msg._type) {
				/* master -> slave */
				case HELLO:
					Slave.sharedSlave()._sid = ((Integer)(msg._content)).intValue();
					System.out.println("Sid got: " + ((Integer)(msg._content)).intValue());
					break;
					
				case NEW_JOB:
					Slave.sharedSlave().newJob((Object[])msg._content);
				/* slave -> master */
				}
			} catch (ClassNotFoundException | IOException e) {
				connAlive = false;
			}
    	}
    }
}
