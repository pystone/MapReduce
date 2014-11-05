/**
 * 
 */
package network;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * @author PY
 *
 */
public class NetworkHelper {
	public static Message receive(Socket socket) throws IOException, ClassNotFoundException {
		ObjectInputStream inStream;
		Object inObj;
		
//		System.out.println("receive msg!");
		inStream = new ObjectInputStream(socket.getInputStream());
		inObj = inStream.readObject();
		if (inObj instanceof Message) {
			Message msg = (Message) inObj;
			return msg;
		}
		
		return null;
	}
	
	public static void send(Socket socket, Message msg) throws IOException {
		if (!(msg instanceof Message)) {
			return;
		}
//		System.out.println("send msg!");
		ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
		out.writeObject(msg);
	}
}
