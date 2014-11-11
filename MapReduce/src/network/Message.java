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
		HELLO,
		NEW_JOB,
		JOB_COMPLETE
	};
	
	public MessageType _type;
	public int _source;
	public Object _content;
}
