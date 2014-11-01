package dfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import mapreduce.MapReduceMaster;
import mapreduce.MapReduceTask;
import message.*;

public class MasterServer {

	public MasterServer() {
		
		this.createWorkingDirectory();

		
		try {
			ServerSocket socketListener = new ServerSocket(YZFS.MASTER_PORT);
			System.out.println("Master server started");

			while (true) {
				Socket socketServing = socketListener.accept();
				System.out.println("Socket accepted from " + socketServing.getInetAddress() + " "
						+ socketServing.getPort());
				MasterServerThread masterThread = new MasterServerThread(socketServing);
				masterThread.start();
			}

		} catch (IOException e) {
			System.err.println("Fail to open socket during master server init.");
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

	private Socket servingSocket = null;
	public static Set<SlaveInfo> slaveList = Collections
			.newSetFromMap(new ConcurrentHashMap<SlaveInfo, Boolean>());
	public static ConcurrentHashMap<String, ArrayList<String>> fileToPart = new ConcurrentHashMap<String, ArrayList<String>>();
	public static ConcurrentHashMap<String, ArrayList<SlaveInfo>> partToSlave = new ConcurrentHashMap<String, ArrayList<SlaveInfo>>();

	// //
	private static boolean ongoing = true;
	public static Queue<MapReduceTask> mapQueue = new LinkedList<MapReduceTask>();
	public static Queue<MapReduceTask> reduceQueue = new LinkedList<MapReduceTask>();
	public static AtomicInteger jobId = new AtomicInteger(0);
	public static HashMap<Integer, Integer> jobToTaskCount = new HashMap<Integer, Integer>();
	// //
}
