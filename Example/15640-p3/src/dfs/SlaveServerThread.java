package dfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

import message.AckMsg;
import message.CatenateMsg;
import message.CopyFromLocalMsg;
import message.Message;
import message.RemoveMsg;

public class SlaveServerThread extends Thread {

	private Socket socketServing = null;

	public SlaveServerThread(Socket socket) {
		this.socketServing = socket;
	}

	public void run() {
		try {
			InputStream inputStream = socketServing.getInputStream();
			ObjectInputStream objInput = new ObjectInputStream(inputStream);

			OutputStream outputStream = socketServing.getOutputStream();
			ObjectOutputStream objOutput = new ObjectOutputStream(outputStream);
			
			/* read a message from the other end */
			Message msg = (Message) objInput.readObject();

			if (msg instanceof CopyFromLocalMsg) {
				System.out.println("slave server receive a copy from local message");
				executeCopyFromLocal((CopyFromLocalMsg) msg);
				AckMsg ack = new AckMsg(true);
				objOutput.writeObject(ack);
				objOutput.flush();
			} else if (msg instanceof RemoveMsg) {
				System.out.println("slave server receive a remove message");
				executeRemove((RemoveMsg) msg);
				AckMsg ack = new AckMsg(true);
				objOutput.writeObject(ack);
				objOutput.flush();
			} else if (msg instanceof CatenateMsg) {
				System.out.println("slave server receive a catenate message");
				executeCatenate((CatenateMsg) msg);
				objOutput.writeObject(msg);
				objOutput.flush();
			}

			this.socketServing.close();
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
	 * put the content of the file into the original msg
	 * 
	 * @param msg
	 */
	private void executeCatenate(CatenateMsg msg) {
		BufferedReader buffer = null;
		StringBuilder ret = new StringBuilder();
		try {
			buffer = new BufferedReader(new FileReader(YZFS.fileSystemWorkingDir
					+ msg.getFilePartName()));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			ret.append("No such file\n");
		}

		String currentLine = null;
		try {
			while ((currentLine = buffer.readLine()) != null) {
				ret.append(currentLine + '\n');
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		msg.setCatReply(ret.toString());
	}

	/**
	 * delete the particular file
	 * 
	 * @param msg
	 */
	private void executeRemove(RemoveMsg msg) {
		File file = new File(YZFS.fileSystemWorkingDir + msg.getFilePartName());
		file.delete();
	}

	/**
	 * fetch the file chunk from client
	 * 
	 * @param msg
	 * @return
	 * @throws IOException
	 */
	private boolean executeCopyFromLocal(CopyFromLocalMsg msg) throws IOException {
		System.out.println("Start File Transfer from " + msg.getFileTransferIP() + " "
				+ msg.getFileTransferPort());
		Socket socket = new Socket(msg.getFileTransferIP(), msg.getFileTransferPort());

		// send the file name and range
		OutputStream output = socket.getOutputStream();
		ObjectOutputStream objOutput = new ObjectOutputStream(output);
		objOutput.writeObject(msg.getFileChunk());
		objOutput.flush();

		InputStream input = socket.getInputStream();

		/*
		 * create the file and write what the server get from socket into the
		 * file
		 */
		FileOutputStream fileOutput = new FileOutputStream(YZFS.fileSystemWorkingDir
				+ msg.getFileName(msg.getFileChunk().localFileFullPath) + ".part"
				+ msg.getFileChunk().partNum);
		byte[] buffer = new byte[YZFS.RECORD_LENGTH];
		int length = -1;
		while ((length = input.read(buffer)) > 0) {
			fileOutput.write(buffer, 0, length);
			fileOutput.flush();
		}

		System.out.println("Finish File Transfer");
		return true;
	}
}
