/**
 * 
 */
package network;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

import mapreduce.Master;

/**
 * Used by master.
 * The listen class responsible for accepting connection from slaves.
 * Running in a standalone thread.
 * 
 * @author PY
 *
 */
public class Listen extends Thread {
	
    private ServerSocket _svrSocket;
    
    public Listen(int port) {
        try {
        	_svrSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void run() {
        while(true) {
                
            Socket slave;
            try {
            	slave = _svrSocket.accept();
            	ObjectInputStream inStream = new ObjectInputStream(slave.getInputStream());
        		Integer slvSid = (Integer)inStream.readObject();
        		
                MsgHandler handler = new MsgHandler(slvSid, slave, Master.sharedMaster());
                Thread t = new Thread(handler);
                t.start();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
        }
        
    }
}
