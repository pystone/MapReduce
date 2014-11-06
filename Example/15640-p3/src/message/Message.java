package message;

import java.io.Serializable;
import java.net.InetAddress;

public class Message implements Serializable {

	private static final long serialVersionUID = -1870514907425528873L;
	protected InetAddress desIP;
	protected int desPort;
	private boolean isFromSlave = false;
	private boolean isFromMapReduceSlave = false;
	
	public void setDes(InetAddress desIP, int desPort) {
		this.desIP = desIP;
		this.desPort = desPort;
	}
	
	public InetAddress getDesIP() {
		return desIP;
	}

	public void setDesIP(InetAddress desIP) {
		this.desIP = desIP;
	}

	public int getDesPort() {
		return desPort;
	}

	public void setDesPort(int desPort) {
		this.desPort = desPort;
	}

	public boolean isFromSlave() {
		return isFromSlave;
	}

	public void setFromSlave(boolean isFromSlave) {
		this.isFromSlave = isFromSlave;
	}

	public boolean isFromMapReduceSlave() {
		return isFromMapReduceSlave;
	}

	public void setFromMapReduceSlave(boolean isFromMapReduceSlave) {
		this.isFromMapReduceSlave = isFromMapReduceSlave;
	}

}
