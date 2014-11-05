/**
 * 
 */
package mapreduce;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import network.Listen;
import network.Message;
import network.NetworkHelper;

/**
 * @author PY
 *
 */
public class Master {
	private static Master _sharedMaster;
	public static Master sharedMaster() {
		if (_sharedMaster == null) {
			_sharedMaster = new Master();
		}
		return _sharedMaster;
	}
	
	private volatile AtomicInteger _sid = null;
	private HashMap<Integer, Socket> _slvSocket = new HashMap<Integer, Socket>();
	private Master() {
		_sid = new AtomicInteger(0);
		Listen l = new Listen(GlobalInfo.sharedInfo().MasterPort);
		l.start();
	}
	
	public void start() {
		
	}
	
	public void newSlave(Socket socket) {
		int sid = _sid.incrementAndGet();
		_slvSocket.put(sid, socket);
		
		Message hello = new Message();
        hello._type = Message.MessageType.HELLO;
        hello._content = sid;  
        try {
			NetworkHelper.send(socket, hello);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
