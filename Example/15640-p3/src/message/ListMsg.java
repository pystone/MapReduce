package message;

public class ListMsg extends Message {

	public String getListReply() {
		return listReply;
	}

	public void setListReply(String listReply) {
		this.listReply = listReply;
	}
	
	private static final long serialVersionUID = 8787138900859307216L;
	private String listReply = null;

}
