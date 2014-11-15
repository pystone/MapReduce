/**
 * 
 */
package mapreduce;

import hdfs.KPFSMaster;
import hdfs.KPFSMasterInterface;
import hdfs.KPFSSlave;
import hdfs.KPFSSlaveInterface;
import hdfs.KPFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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

	private KPFSMaster _kpfsMaster;
	
	public void start() {
		/* start HDFS */
		_kpfsMaster = new KPFSMaster();
		try {
			KPFSMasterInterface stub = (KPFSMasterInterface) UnicastRemoteObject
					.exportObject(_kpfsMaster, 0);
			Registry registry = null;
			try {
				registry = LocateRegistry
						.getRegistry(GlobalInfo.sharedInfo().DataMasterPort);
				registry.list();
			} catch (RemoteException e) {
				registry = LocateRegistry.createRegistry(GlobalInfo.sharedInfo().DataMasterPort);
			}
			registry.bind("KPFSMasterInterface", stub);

			System.out.println("KPFS master ready");
		} catch (RemoteException | AlreadyBoundException e) {
			e.printStackTrace();
		}
		
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
			newTask(cmd[1], cmd[2], cmd[3], cmd[4]);
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

	private Task currentTask = null;
	private HashMap<Integer, Boolean> mapCheckList = new HashMap<Integer, Boolean>();
	private HashMap<Integer, Boolean> reduceCheckList = new HashMap<Integer, Boolean>();

	public void newTask(String inputFile, String mapperPath,
			String reducerPath, String taskName) {
		int taskId = _taskId.incrementAndGet();
		currentTask = new Task(taskId, taskName);

		String dirPath = inputFile.substring(0, inputFile.lastIndexOf('/') + 1);
		String fileName = inputFile.substring(inputFile.lastIndexOf("/") + 1);
		String interDir = dirPath + GlobalInfo.sharedInfo().IntermediateDirName
				+ "/";
		String chunkDir = dirPath + GlobalInfo.sharedInfo().ChunkDirName + "/";
		String resultDir = dirPath + GlobalInfo.sharedInfo().ResultDirName
				+ "/";

		/* create the folder to store intermediate files */
		(new File(interDir)).mkdirs();
		(new File(chunkDir)).mkdirs();
		(new File(resultDir)).mkdirs();
		
		File jar = new File(GlobalInfo.sharedInfo().JarFilePath, GlobalInfo.sharedInfo().JarFileName);
		_kpfsMaster.addFileLocation(mapperPath, GlobalInfo.sharedInfo().DataMasterHost, (int)jar.length());
		
		ArrayList<String> files = _kpfsMaster.splitFile(inputFile,
				GlobalInfo.sharedInfo().FileChunkSizeB, chunkDir, fileName);
		int jobId = 0;
		for (String fn : files) {
			JobInfo job = new JobInfo(++jobId, taskName);
			job._mrFile = new KPFile(GlobalInfo.sharedInfo().JarFilePath, GlobalInfo.sharedInfo().JarFileName);
			job._taskId = taskId;
			job._sid = getFreeSlave();
			job._type = JobInfo.JobType.MAP;

			ArrayList<KPFile> list = new ArrayList<KPFile>();
			list.add(new KPFile(chunkDir, fn));
			job._inputFile = list;

			currentTask._jobs.put(jobId, job);
			mapCheckList.put(jobId, false);
		}

		JobManager.sharedJobManager().sendJobs(currentTask._jobs.values());
	}

	public void checkMapCompleted(JobInfo job) {
		boolean isMapCompleted = true;
		if (mapCheckList != null) {
			mapCheckList.put(job._jobId, true);

			Iterator<Map.Entry<Integer, Boolean>> itor = mapCheckList
					.entrySet().iterator();
			while (itor.hasNext()) {
				Map.Entry<Integer, Boolean> entry = (Map.Entry<Integer, Boolean>) itor
						.next();
				boolean flag = (boolean) entry.getValue();
				if (!flag) {
					isMapCompleted = false;
				}
			}
		}

		if (isMapCompleted) {
			int jobId = 0;
			for (int i = 0; i < GlobalInfo.sharedInfo().NumberOfReducer; i++) {
				JobInfo reduceJob = new JobInfo(++jobId, currentTask._taskName);
				reduceJob._mrFile = new KPFile(GlobalInfo.sharedInfo().JarFilePath, GlobalInfo.sharedInfo().JarFileName);
				reduceJob._taskId = currentTask._taskId;
				reduceJob._sid = getFreeSlave();
				reduceJob._type = JobInfo.JobType.REDUCE;
				reduceJob._inputFile = job._outputFile;
				currentTask._jobs.put(jobId, job);
				reduceCheckList.put(jobId, false);
			}
			JobManager.sharedJobManager().sendJobs(currentTask._jobs.values());
		}
	}

	public void checkReduceCompleted(JobInfo job) {
		boolean isReduceCompleted = true;
		if (reduceCheckList != null) {
			reduceCheckList.put(job._jobId, true);

			Iterator<Map.Entry<Integer, Boolean>> itor = reduceCheckList
					.entrySet().iterator();
			while (itor.hasNext()) {
				Map.Entry<Integer, Boolean> entry = (Map.Entry<Integer, Boolean>) itor
						.next();
				boolean flag = (boolean) entry.getValue();
				if (!flag) {
					isReduceCompleted = false;
				}
			}
		}

		if (isReduceCompleted) {
			System.out.println("Task is completed!");
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
