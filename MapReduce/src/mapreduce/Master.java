/**
 * 
 */
package mapreduce;

import hdfs.HDFileSplit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import jobcontrol.JobInfo;
import jobcontrol.JobManager;
import jobcontrol.Task;
import network.Listen;
import network.Message;
import network.NetworkHelper;

/**
 * @author PY
 *
 */
public class Master {
	private static Master _sharedMaster;
	public static Master sharedMaster() {
		if (_sharedMaster == null) {
			_sharedMaster = new Master();
		}
		return _sharedMaster;
	}
	
	private volatile AtomicInteger _sid = null;
	private volatile AtomicInteger _taskId = null;
	public HashMap<Integer, Socket> _slvSocket = new HashMap<Integer, Socket>();
	private Master() {
		_sid = new AtomicInteger(0);
		_taskId = new AtomicInteger(0);
		Listen l = new Listen(GlobalInfo.sharedInfo().MasterPort);
		l.start();
	}
	
	public void start() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line = null;
			try {
				line = br.readLine();
				String[] cmd = line.split(" ");
				System.out.println(cmd[0]);
				inputHandler(cmd);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	public void inputHandler(String[] cmd) {
		switch (cmd[0]) {
		case "new":
			newTask(cmd[1], cmd[2], cmd[3]);
			break;
		}
	}
	
	public void newSlave(Socket socket) {
		int sid = _sid.incrementAndGet();
		_slvSocket.put(sid, socket);
		
		Message hello = new Message();
        hello._type = Message.MessageType.HELLO;
        hello._content = sid;  
        try {
			NetworkHelper.send(socket, hello);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void newTask(String inputFile, String mapperPath, String reducerPath) {
		int taskId = _taskId.incrementAndGet();
		Task task = new Task(taskId);
		task._mapperPath = mapperPath;
		task._reducerPath = reducerPath;
		
		String dirPath = inputFile.substring(0, inputFile.lastIndexOf('/') + 1); /* ending with / */
		String fileName = inputFile.substring(inputFile.lastIndexOf("/") + 1);
		String interDir = dirPath + GlobalInfo.sharedInfo().IntermediateDirName + "/";
		String chunkDir = dirPath + GlobalInfo.sharedInfo().ChunkDirName + "/";
		String resultDir = dirPath + GlobalInfo.sharedInfo().ResultDirName + "/";
		
		(new File(interDir)).mkdirs();	/* create the folder to store intermediate files */
		(new File(chunkDir)).mkdirs();
		(new File(resultDir)).mkdirs();
		
		try {
			ArrayList<String> files = HDFileSplit.split(inputFile, GlobalInfo.sharedInfo().FileChunkSizeB, chunkDir, fileName);
			int jobid = 0;
			for (String fn: files) {
				JobInfo job = new JobInfo(++jobid);
				job._taskId = taskId;
				job._sid = getFreeSlave();
				job._inFilePath = fn;
				job._outFileDir = resultDir;
				job._interFileDir = interDir;
				job._mapperPath = mapperPath;
				job._reducerPath = reducerPath;
				job._type = JobInfo.JobType.MAP;
				
				task._jobs.put(jobid, job);
			}
			
			JobManager.sharedJobManager().sendJobs(task._jobs.values());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/* load balancer */
	public int getFreeSlave() {
		Random rand = new Random();
		int slv = rand.nextInt(_slvSocket.size());
		Integer sid = (Integer) _slvSocket.keySet().toArray()[slv];
		return sid.intValue();
	}
	
}
