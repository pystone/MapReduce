/**
 * 
 */
package network;

import java.io.Serializable;

/**
 * @author PY
 *
 */
public class Message implements Serializable {
	private static final long serialVersionUID = 1114115388371865795L;
	
	public enum MessageType {
		HELLO_SID,
		HELLO_ACK,
		NEW_JOB,
		MAP_COMPLETE,
		REDUCE_COMPLETE,
		SLAVE_HEARTBEAT,
		JOB_UPDATE
	};
	
	public MessageType _type;
	public int _source;
	public Object _content;
	
	public Message() {
		
	}
	public Message(int sid, MessageType type) {
		_type = type;
		_source = sid;
	}
}
