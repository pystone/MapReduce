package dfs;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;

public class SlaveInfo implements Serializable {

/**
	 * 
	 */
	private static final long serialVersionUID = 5527902893202418272L;
	//	public Socket socket;
	public InetAddress iaddr;
	public int port;
	
//	public InputStream input;
//	public OutputStream output;
	
	public String toString() {
		return "\tInetAddress: " + iaddr + "\tport number: " + port;
	}
}
