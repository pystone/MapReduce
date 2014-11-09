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
	
	/*
	 * ATTENTION: before using this function, make sure you will get the response right away, 
	 * otherwise you will get stuck here. It's a block function.
	 */
	public static Message sendAndReceive(Socket socket, Message msg) throws IOException, ClassNotFoundException {
		/* send */
		if (!(msg instanceof Message)) {
			return null;
		}
		ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
		out.writeObject(msg);
		
		/* receive */
		ObjectInputStream inStream;
		Object inObj;
		
		inStream = new ObjectInputStream(socket.getInputStream());
		inObj = inStream.readObject();
		if (inObj instanceof Message) {
			Message inMsg = (Message) inObj;
			return inMsg;
		}
		
		return null;
		
	}
}
