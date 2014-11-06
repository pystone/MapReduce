package dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import mapreduce.MapReduceTask;
import message.AckMsg;
import message.CatenateMsg;
import message.CopyFromLocalMsg;
import message.DownloadFileMsg;
import message.ListMsg;
import message.Message;
import message.QuitMsg;
import message.RemoveMsg;

public class MasterServerThread extends Thread {

	public MasterServerThread(Socket socket) throws UnknownHostException {
		this.socketServing = socket;
		this.masterIP = InetAddress.getLocalHost();
	}

	public void run() {
		try {
			InputStream inputStream = socketServing.getInputStream();
			ObjectInputStream input = new ObjectInputStream(inputStream);

			OutputStream outputStream = socketServing.getOutputStream();
			ObjectOutputStream output = new ObjectOutputStream(outputStream);

			/* read a message from the other end */
			Message msg = (Message) input.readObject();

			/*
			 * if the incoming msg is from a slave server, save all the
			 * information into the list for future use
			 */
			if (msg.isFromSlave()) {
				SlaveInfo slaveInfo = new SlaveInfo();
				slaveInfo.iaddr = socketServing.getInetAddress();
				slaveInfo.port = YZFS.SLAVE_PORT;
				MasterServer.slaveList.add(slaveInfo);
				System.out.println("One slave added");

				/*
				 * dont send msg back to slave, keeps slave reading from the
				 * stream so that slave server can quit upon master servers
				 * exception
				 */
				return;
			} else if (msg.isFromMapReduceSlave()) {
				return;
			}

			/* send the reply msg after doing all the executions */
			Message reply = parseMessage(msg);
			output.writeObject(reply);
			output.flush();

			socketServing.close();
			System.out.println("reply msg sent from master");

		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Fail to accept slave server request.");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * dispatch the message to particular method and return the reply msg
	 * 
	 * @param msg
	 * @return
	 */
	private Message parseMessage(Message msg) throws IOException, ClassNotFoundException {
		if (msg instanceof CopyFromLocalMsg) {
			System.out.println("master server receive a copy form local message");
			executeCopyFromLocal((CopyFromLocalMsg) msg);
			return new AckMsg(true);
		} else if (msg instanceof ListMsg) {
			System.out.println("master server receive a list message");
			executeList((ListMsg) msg);
			return msg;
		} else if (msg instanceof RemoveMsg) {
			System.out.println("master server receive a remove message");
			executeRemove((RemoveMsg) msg);
			return new AckMsg(true);
		} else if (msg instanceof CatenateMsg) {
			System.out.println("master server receive a cat message");
			executeCatenate((CatenateMsg) msg);
			return msg;
		} else if (msg instanceof DownloadFileMsg) {
			System.out.println("master server receive a downloadfile message");
			executeDownloadFileMsg((DownloadFileMsg) msg);
			return msg;
		} else if (msg instanceof QuitMsg) {
			System.exit(0);
		}
		return null;
	}

	/**
	 * get cat message from each part of the file and glue them together
	 * 
	 * @param msg
	 */
	private void executeCatenate(CatenateMsg msg) throws UnknownHostException, IOException,
			ClassNotFoundException {
		ArrayList<String> filePartList = MasterServer.fileToPart.get(msg.getFileName());
		StringBuilder strReply = new StringBuilder();

		/* get cat message from each part of the file and glue them together */
		for (String filePartName : filePartList) {
			SlaveInfo slaveInfo = MasterServer.partToSlave.get(filePartName).get(0);
			msg.setFilePartName(filePartName);
			Message reply = CommunicationModule.sendMessage(slaveInfo.iaddr, YZFS.SLAVE_PORT, msg);
			strReply.append(((CatenateMsg) reply).getCatReply());
		}
		msg.setCatReply(strReply.toString());
	}

	/**
	 * remove the specific file one each slave server and update the fileList &
	 * partList
	 * 
	 * @param msg
	 */
	private void executeRemove(RemoveMsg msg) throws UnknownHostException, IOException,
			ClassNotFoundException {
		String fileName = msg.getFileName();
		ArrayList<String> partList = new ArrayList<String>();
		if (fileName.equals("all")) {
			for (String tmpFileName : MasterServer.fileToPart.keySet())
				partList.addAll(MasterServer.fileToPart.get(tmpFileName));
		} else {
			partList = MasterServer.fileToPart.get(fileName);
		}

		/* remove each file part on every slave server */
		for (String filePartName : partList) {
			msg.setFilePartName(filePartName);
			ArrayList<SlaveInfo> slaveList = MasterServer.partToSlave.get(filePartName);
			for (SlaveInfo slaveInfo : slaveList) {
				CommunicationModule.sendMessage(slaveInfo.iaddr, YZFS.SLAVE_PORT, msg);
			}

			/* update the data structure */
			MasterServer.partToSlave.remove(filePartName);
		}

		/* update the data structure */
		MasterServer.fileToPart.remove(fileName);
	}

	/**
	 * list all files on the YZFS, store the reply String in the original msg
	 * 
	 * @param msg
	 */
	private void executeList(ListMsg msg) {
		StringBuilder strReply = new StringBuilder();
		for (String str : MasterServer.fileToPart.keySet())
			strReply.append(str + '\t');
		strReply.insert(0, "Found " + MasterServer.fileToPart.size() + " items\n");
		msg.setListReply(strReply.toString());
	}

	/**
	 * tell random slaves to fetch file(s) or part of the file(s) from client
	 * side
	 * 
	 * @param msg
	 */
	private void executeCopyFromLocal(CopyFromLocalMsg msg) throws IOException,
			ClassNotFoundException {

		/* send copy from local msg for every file in the file list */
		ArrayList<String> fileList = msg.getLocalFileListFullPath();
		ArrayList<Long> fileSize = msg.getLocalFileSize();
		int length = fileSize.size();
		for (int i = 0; i < length; i++) {
			FilePartition filePartition = new FilePartition(fileList.get(i), fileSize.get(i));
			ArrayList<FileChunk> chunkList = filePartition.generateFileChunks();
			ArrayList<String> partList = new ArrayList<String>();

			/* send copy from local msg for every part of the file */
			for (FileChunk chunk : chunkList) {
				ArrayList<SlaveInfo> randomSlaveList = getRandomSlaves();
				msg.setFileChunk(chunk);
				Message ack = new Message();
				for (SlaveInfo slaveInfo : randomSlaveList) {
					ack = CommunicationModule.sendMessage(slaveInfo.iaddr, YZFS.SLAVE_PORT, msg);
					if (ack instanceof AckMsg)
						System.out.println("ack from slave server");
				}
				/* add to partToSlave list */
				int partNum = chunk.partNum;
				String filePartName = msg.getFileName(fileList.get(i)) + ".part" + partNum;
				MasterServer.partToSlave.put(filePartName, randomSlaveList);
				partList.add(filePartName);

				System.out.println("send message to " + randomSlaveList.size() + " hosts");
			}
			/* add to fileToPart list */
			MasterServer.fileToPart.put(msg.getFileName(fileList.get(i)), partList);

		}

	}

	public void executeDownloadFileMsg(DownloadFileMsg msg) throws IOException {
		// ??? can use thread here to improve performace
		System.out.println("Start File Download from " + msg.getDesIP() + " " + msg.getDesPort());
		Socket socket = new Socket(msg.getDesIP(), msg.getDesPort());

		InputStream input = socket.getInputStream();

		/*
		 * create the file and write what the server get from socket into the
		 * file
		 */
		System.out.println(msg.getFileFullPath());

		// ??? if YZFS/ doesn't exist, exception throw here
		FileOutputStream fileOutput = new FileOutputStream(msg.getFileFullPath());

		byte[] buffer = new byte[1024];
		int length = -1;
		while ((length = input.read(buffer)) > 0) {
			fileOutput.write(buffer, 0, length);
			fileOutput.flush();
		}

		System.out.println("Finish File Downlaod");
		msg.setSuccessful(true);
		socket.close();
		input.close();
		fileOutput.close();

		Socket ackSock = new Socket(masterIP, YZFS.MP_PORT);
		ObjectOutputStream ackOutput = new ObjectOutputStream(ackSock.getOutputStream());

		ackOutput.writeObject(msg.getTask());
		ackOutput.flush();

	}

	/**
	 * get random slaves from slave list, return all slaves if replication
	 * factor is greater than the number of slaves
	 * 
	 * @return
	 */
	private ArrayList<SlaveInfo> getRandomSlaves() {
		ArrayList<SlaveInfo> ret = new ArrayList<SlaveInfo>(MasterServer.slaveList);
		if (YZFS.replicationFactor < MasterServer.slaveList.size()) {
			Collections.shuffle(ret);
			return new ArrayList<SlaveInfo>(ret.subList(0, (YZFS.replicationFactor)));
		} else {
			return ret;
		}
	}

	private Socket socketServing = null;
	private InetAddress masterIP;

}
