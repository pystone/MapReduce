package dfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;

import message.*;

/**
 * This is a background running file system slave service
 * 
 * @author zhengk
 */
public class SlaveServer {

	public SlaveServer(String masterHostName) {
		System.out.println("Slave server started");
		this.createWorkingDirectory();
		ConnectMaster connectMaster = new ConnectMaster(masterHostName);
		connectMaster.start();
		
		StartService startService = new StartService();
		startService.start();
	}

	/**
	 * The only thing it does is to tell master to add its ip to slavelist
	 * @author zhengk
	 *
	 */
	private class ConnectMaster extends Thread {

		public ConnectMaster(String masterHostName) {
			this.masterHostName = masterHostName;
		}

		public void run() {
			/* get connection to master server */
			System.out.println(masterHostName);

			/*
			 * after successfully connect to master server, write master
			 * hostname and port number into a file, so that file system command
			 * line process can utilize it
			 */
			Message msg = null;
			try {
				this.writeMasterInfo(masterHostName, YZFS.MASTER_PORT);
				msg = new Message();
				msg.setDes(InetAddress.getByName(masterHostName), YZFS.MASTER_PORT);
				msg.setFromSlave(true);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			try {
				CommunicationModule.sendMessage(msg);
			} catch (Exception e) {
				System.out.println("slave quit with master");
				System.exit(0);
			}
		}


		private void writeMasterInfo(String masterHostName, int masterPort)
				throws FileNotFoundException, IOException {
			Properties prop = new Properties();

			// set the properties value
			prop.setProperty("master host name", masterHostName);
			prop.setProperty("master port number", "" + masterPort);

			// save properties to project root folder
			prop.store(new FileOutputStream(YZFS.fileSystemWorkingDir + ".masterinfo.config"), null);

		}

		private String masterHostName = null;
	}

	
	/**
	 * Do the actual communication with master server
	 * @author zhengk
	 *
	 */
	public class StartService extends Thread {

		public void run() {
			ServerSocket socketListener = null;
			try {
				socketListener = new ServerSocket(YZFS.SLAVE_PORT);

				while (true) {
					Socket socketServing = null;
					socketServing = socketListener.accept();

					System.out.println("Socket accepted from " + socketServing.getInetAddress()
							+ " " + socketServing.getPort());
					SlaveServerThread slaveThread = new SlaveServerThread(socketServing);
					slaveThread.start();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void createWorkingDirectory() {
		File folder = new File(YZFS.fileSystemWorkingDir);
		/* create working directory */
		if (!folder.exists()) {
			if (folder.mkdir()) {
				System.out.println("Working Directory is created!");
			} else {
				System.err.println("Failed to create directory!");
			}
		}
		/* delete all files in the directory */
		else {
			File[] listOfFiles = folder.listFiles();
			for (File file : listOfFiles)
				file.delete();
		}
	}
}
