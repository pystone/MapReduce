/**
 * 
 */
package mapreduce;

import hdfs.KPFSException;
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
import jobcontrol.UnfinishedJobs;
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

	public HashMap<Integer, Socket> _slvSocket = new HashMap<Integer, Socket>();
	public volatile HashMap<String, Task> _tasks = new HashMap<String, Task>();
	private volatile UnfinishedJobs _unfinishedMap = new UnfinishedJobs();
	private volatile UnfinishedJobs _unfinishedReduce = new UnfinishedJobs();


	private Master() {
		GlobalInfo.sharedInfo()._sid = 0;
		Listen l = new Listen(GlobalInfo.sharedInfo().MasterPort);
		l.start();
	}

	private KPFSMasterInterface _kpfsMaster;
	private KPFSSlaveInterface _kpfsSlave;
	
	public void start() {
		/* start HDFS as master node*/
		try {
			_kpfsMaster = new KPFSMaster();
            KPFSMasterInterface stub = (KPFSMasterInterface) UnicastRemoteObject.exportObject(_kpfsMaster, 
            		GlobalInfo.sharedInfo().DataMasterPort);
            Registry registry = LocateRegistry.createRegistry(GlobalInfo.sharedInfo().DataMasterPort);;
            registry.rebind("DataMaster", stub);
            System.out.println("KPFS master ready");
        } catch (Exception e) {
            System.err.println("Failed to establish as HDFS master!");
            e.printStackTrace();
            System.exit(-1);
        }
		
		/* start HDFS as data node */
		try {
			_kpfsSlave = new KPFSSlave();
            KPFSSlaveInterface stub = (KPFSSlaveInterface) UnicastRemoteObject.exportObject(_kpfsSlave, 
            		GlobalInfo.sharedInfo().DataSlavePort);
            Registry registry = LocateRegistry.createRegistry(GlobalInfo.sharedInfo().getDataSlavePort(0));;
            registry.rebind("DataSlave", stub);
            System.out.println("KPFS data node ready");
        } catch (Exception e) {
            System.err.println("Failed to establish as HDFS master!");
            e.printStackTrace();
            System.exit(-1);
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
			newTask(cmd[1]);
			break;
		}
	}

	public void newSlave(Socket socket, int sid) {
		// TODO: check the incoming slave is on the config file!
		_slvSocket.put(sid, socket);
		System.out.println("New slave connected! " + sid);
	}

	public void newTask(String taskName) {
		Task currentTask = new Task(taskName);
		_tasks.put(taskName, currentTask);

		String rootDir = GlobalInfo.sharedInfo().MasterRootDir;
		String interDir = rootDir + taskName + "/" + GlobalInfo.sharedInfo().IntermediateDirName
				+ "/";
		String chunkDir = rootDir + taskName + "/" + GlobalInfo.sharedInfo().ChunkDirName + "/";
		String resultDir = rootDir + taskName + "/" + GlobalInfo.sharedInfo().ResultDirName
				+ "/";

		/* create the folder to store intermediate files */
		(new File(interDir)).mkdirs();
		(new File(chunkDir)).mkdirs();
		(new File(resultDir)).mkdirs();
		
		String jarPath = rootDir + taskName + "/" + GlobalInfo.sharedInfo().UserDirName + "/" + taskName + ".jar";
		String inputPath = rootDir + taskName + "/" + GlobalInfo.sharedInfo().UserDirName + "/" + taskName + ".txt";
		long jarSize = (new File(jarPath)).length();
		KPFile jarFile = new KPFile(taskName + "/" + GlobalInfo.sharedInfo().UserDirName + "/", taskName + ".jar");
		try {
			/* 0 is the id of master */
			_kpfsMaster.addFileLocation(jarFile.getRelPath(), 0, jarSize);
		} catch (RemoteException e) {
			System.err.println("Failed to add file!");
			e.printStackTrace();
		}
		
		currentTask._mrFile = jarFile;
		
		ArrayList<String> files = ((KPFSMaster) _kpfsMaster).splitFile(inputPath,
					GlobalInfo.sharedInfo().FileChunkSizeB, chunkDir, taskName);
		
		int jobId = 0;
		for (String fn : files) {
			JobInfo job = new JobInfo(++jobId, taskName);
			job._mrFile = jarFile;
			job._sid = getFreeSlave();
			job._type = JobInfo.JobType.MAP;

			ArrayList<KPFile> list = new ArrayList<KPFile>();
			if(fn.contains("/")) {
				String[] parts = fn.split("/");
				KPFile kpfile = new KPFile(taskName + "/" + GlobalInfo.sharedInfo().ChunkDirName + "/", parts[parts.length - 1]);
				list.add(kpfile);
				try {
					/* 0 is the id of master */
					_kpfsMaster.addFileLocation(kpfile.getRelPath(), 0, (new File(fn)).length());
				} catch (RemoteException e) {
					System.err.println("ERROR: failed to add chunk file (" + fn + ") into data master!");
					e.printStackTrace();
				}
			}
			job._inputFile = list;
			
			/* duplicate the files to some other data nodes */
			try {
				((KPFSMaster) _kpfsMaster).duplicateFiles(list, _slvSocket.keySet().toArray());
			} catch (IOException | KPFSException e) {
				System.out.println("ERROR: failed to duplicate files!");
				e.printStackTrace();
			}

			currentTask._jobs.put(jobId, job);
		}
		
		
		currentTask._phase = Task.TaskPhase.MAP;
		JobManager.sharedJobManager().sendJobs(currentTask._jobs.values());
	}

	public void checkMapCompleted(JobInfo job) {
		Task task = _tasks.get(job._taskName);
		if (task == null) {
			System.out.println("WARNING: receiving a job that does not belong to any working task!");
			return;
		}
		task._jobs.put(job._jobId, job);
		
//		System.out.println("Job finished!");
//		job.serialize();
		
		// TODO: put into another thread
		if (task.phaseComplete()) {
			HashMap<Integer, JobInfo> jobs = new HashMap<Integer, JobInfo>();
			
			String interDir = task._taskName + "/" + GlobalInfo.sharedInfo().IntermediateDirName + "/";
			
			HashMap<Integer, ArrayList<KPFile>> interFiles = new HashMap<Integer, ArrayList<KPFile>>();
			
			for (JobInfo ji: task._jobs.values()) {
				HashMap<Integer, KPFile> files = ji.getInterFilesWithIndex();
				for (Integer idx: files.keySet()) {
					ArrayList<KPFile> farr = interFiles.get(idx);
					if (farr==null) {
						farr = new ArrayList<KPFile>();
						interFiles.put(idx, farr);
					}
					farr.add(files.get(idx));
				}
			}
			
			for (Integer idx: interFiles.keySet()) {
				JobInfo reduceJob = new JobInfo(idx, task._taskName);
				reduceJob._mrFile = task._mrFile;
				reduceJob._sid = getFreeSlave();
				reduceJob._type = JobInfo.JobType.REDUCE;
				reduceJob._inputFile = interFiles.get(idx);
				jobs.put(idx, reduceJob);
			}
			
			task._phase = Task.TaskPhase.REDUCE;
			task._jobs = jobs;
			JobManager.sharedJobManager().sendJobs(task._jobs.values());
		}
		
	}

	public void checkReduceCompleted(JobInfo job) {
		Task task = _tasks.get(job._taskName);
		if (task == null) {
			System.out.println("WARNING: receiving a job that does not belong to any working task!");
			return;
		}
		task._jobs.put(job._jobId, job);
		
		if (task.phaseComplete()) {
			System.out.println("Task " + task._taskName + " is completed! The output files are at: ");
			for (JobInfo ji: task._jobs.values()) {
				System.out.println("\tSlave " + ji._sid + ": " + ji._outputFile.get(0).getRelPath());
			}
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
