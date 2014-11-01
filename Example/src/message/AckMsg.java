package message;

import java.net.InetAddress;

public class AckMsg extends Message{
	
	public AckMsg(boolean isSuccessful) {
		this.isSuccessful = isSuccessful;
	}
	
	public AckMsg(InetAddress desIP, int desPort) {
		this.desIP = desIP;
		this.desPort = desPort;
	}

	public boolean isSuccessful() {
		return isSuccessful;
	}
	public void setSuccessful(boolean isSuccessful) {
		this.isSuccessful = isSuccessful;
	}

	private static final long serialVersionUID = -4294820880518030645L;
	private boolean isSuccessful = false;
	
}
