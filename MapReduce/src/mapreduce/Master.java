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
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import jobcontrol.JobInfo;
import jobcontrol.JobManager;
import jobcontrol.Task;
import network.Listen;
import network.Message;
import network.NetworkFailInterface;
import network.NetworkHelper;

/**
 * @author PY
 * 
 */
public class Master implements NetworkFailInterface {
	private static Master _sharedMaster;

	public static Master sharedMaster() {
		if (_sharedMaster == null) {
			_sharedMaster = new Master();
		}
		return _sharedMaster;
	}

	public HashMap<Integer, Socket> _slvSocket = new HashMap<Integer, Socket>();
	public volatile HashMap<String, Task> _tasks = new HashMap<String, Task>();
	private volatile HashMap<Integer, SlaveTracker> _slvTracker = new HashMap<Integer, SlaveTracker>();


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
		case "showslave":
			showSlave();
			break;
		case "showalltask":
			showAllTask();
			break;
		case "show":
			showTask(cmd[2]);
			break;
		}
			
	}
	
	private void showSlave() {
		
	}
	
	private void showAllTask() {
		
	}
	
	private void showTask(String taskName) {
		
	}

	public void newSlave(Socket socket, int sid) {
		// TODO: check the incoming slave is on the config file!
		_slvSocket.put(sid, socket);
		System.out.println("New slave connected! " + sid);

		Message ack = new Message(0, Message.MessageType.HELLO_ACK);
		try {
			NetworkHelper.send(socket, ack);
		} catch (IOException e) {
			slaveDown(sid);
		}
	}
	
	public void slaveDown(int sid) {
		System.out.println(sid + "is down...");
	}
	
	public void slaveHeartbeat(int sid, SlaveTracker tracker) {
//		System.out.println("heartbeat from " + sid);
		synchronized (_slvTracker) {
			_slvTracker.put(sid, tracker);
		}
	}

	public void newTask(String taskName) {
		String rootDir = GlobalInfo.sharedInfo().MasterRootDir;
		String jarPath = rootDir + taskName + "/" + GlobalInfo.sharedInfo().UserDirName + "/" + taskName + ".jar";
		String inputPath = rootDir + taskName + "/" + GlobalInfo.sharedInfo().UserDirName + "/" + taskName + ".txt";
		File jarf = new File (jarPath);
		File inf = new File (inputPath);
		if (!jarf.exists() || !inf.exists()) {
			System.out.println("Please put " + taskName + ".txt and " + taskName + ".jar into the right directory and try again!" );
			return;
		}
		
		Task currentTask = new Task(taskName);
		_tasks.put(taskName, currentTask);

		
		String interDir = rootDir + taskName + "/" + GlobalInfo.sharedInfo().IntermediateDirName
				+ "/";
		String chunkDir = rootDir + taskName + "/" + GlobalInfo.sharedInfo().ChunkDirName + "/";
		String resultDir = rootDir + taskName + "/" + GlobalInfo.sharedInfo().ResultDirName
				+ "/";

		/* create the folder to store intermediate files */
		(new File(interDir)).mkdirs();
		(new File(chunkDir)).mkdirs();
		(new File(resultDir)).mkdirs();
		
		
		long jarSize = jarf.length();
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
			job._type = JobInfo.JobType.MAP_READY;

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

			currentTask._mapJobs.put(jobId, job);
		}
		
		
		currentTask._phase = Task.TaskPhase.MAP;
		JobManager.sharedJobManager().sendJobs(currentTask._mapJobs.values());
	}

	public void checkMapCompleted(JobInfo job) {
		Task task = _tasks.get(job._taskName);
		if (task == null) {
			System.out.println("WARNING [checkMapCompleted]: receiving a job that does not belong to any working task!");
			return;
		}
		JobInfo oldJob = task._mapJobs.get(job._jobId);
		if (oldJob == null) {
			return;
		}
		if (task._phase!=Task.TaskPhase.MAP || oldJob==null || oldJob._sid!=job._sid) {
			System.out.println("Getting an old finished job. Ignoring it. " + job._taskName + job._jobId + job._type + " from " + job._sid);
			if (oldJob != null) {
				System.out.println("\tOld job: " + oldJob._taskName + oldJob._jobId + oldJob._type + " from " + oldJob._sid);
			}
			return;
		}

		
		task._mapJobs.put(job._jobId, job);
		
//		System.out.println("Job finished!");
//		job.serialize();
		
		if (task.phaseComplete()) {
			HashMap<Integer, JobInfo> jobs = new HashMap<Integer, JobInfo>();
			
			HashMap<Integer, ArrayList<KPFile>> interFiles = new HashMap<Integer, ArrayList<KPFile>>();
			
			for (JobInfo ji: task._mapJobs.values()) {
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
				reduceJob._type = JobInfo.JobType.REDUCE_READY;
				reduceJob._inputFile = interFiles.get(idx);
				jobs.put(idx, reduceJob);
			}
			
			task._phase = Task.TaskPhase.REDUCE;
			task._reduceJobs = jobs;
			JobManager.sharedJobManager().sendJobs(task._reduceJobs.values());
		}
		
	}

	public void checkReduceCompleted(JobInfo job) {
		Task task = _tasks.get(job._taskName);
		if (task == null) {
			System.out.println("WARNING [checkReduceCompleted]: receiving a job that does not belong to any working task!");
			return;
		}
		
		JobInfo oldJob = task._reduceJobs.get(job._jobId);
		if (oldJob == null) {
			return;
		}
		if (task._phase!=Task.TaskPhase.REDUCE || oldJob==null || oldJob._sid!=job._sid) {
			System.out.println("Getting an old finished job. Ignoring it. " + job._taskName + job._jobId + job._type + " from " + job._sid);
			if (oldJob != null) {
				System.out.println("\tOld job: " + oldJob._taskName + oldJob._jobId + oldJob._type + " from " + oldJob._sid);
			}
			return;
		}
		
		task._reduceJobs.put(job._jobId, job);
		
		if (task.phaseComplete()) {
			task._phase = Task.TaskPhase.FINISH;
			_tasks.remove(task);
			System.out.println("Task " + task._taskName + " is completed! The output files are at: ");
			for (JobInfo ji: task._reduceJobs.values()) {
				System.out.println("\tSlave " + ji._sid + ": " + ji._outputFile.get(0).getRelPath());
			}
		}
		
		
	}
	
	public void jobUpdate(JobInfo job) {
		Task task = _tasks.get(job._taskName);
		if (task == null) {
			System.out.println("WARNING [jobUpdate]: receiving a job that does not belong to any working task!");
			return;
		}
		
		JobInfo oldJob = null;
		if (task._phase == Task.TaskPhase.MAP) {
			oldJob = task._mapJobs.get(job._jobId);
		} else if (task._phase == Task.TaskPhase.REDUCE) {
			oldJob = task._reduceJobs.get(job._jobId);
		}
		
		if (oldJob==null || oldJob._sid!=job._sid) {
			System.out.println("Getting an old finished job. Ignoring it. " + job._taskName + job._jobId + job._type + " from " + job._sid);
			if (oldJob != null) {
				System.out.println("\tOld job: " + oldJob._taskName + oldJob._jobId + oldJob._type + " from " + oldJob._sid);
			}
			return;
		}
		
		System.out.println("[jobUpdate] " + job._taskName + job._jobId + job._type + " from " + job._sid);
		if (task._phase == Task.TaskPhase.MAP) {
			task._mapJobs.put(job._jobId, job);
		} else if (task._phase == Task.TaskPhase.REDUCE) {
			task._reduceJobs.put(job._jobId, job);
		}
	}

	/* load balancer */
	public synchronized int getFreeSlave() {
		int slv = 0;
		synchronized (_slvSocket) {
			if ((slv = _slvSocket.size()) == 0) {
				System.out.println("Currently all slaves are down. Please restart this system!");
				System.exit(-1);
			}
		}
		Random rand = new Random();
		slv = rand.nextInt(slv);
		
		Integer sid = 0;
		synchronized (_slvSocket) {
			sid = (Integer) _slvSocket.keySet().toArray()[slv];
		}
		return sid.intValue();
	}

	@Override
	public void networkFail(int sid) {
		System.out.println("networkFail: " + sid);
		if (_slvSocket.containsKey(sid) == false) {
			System.out.println("WARNING! no such sid.");
			return;
		}
		
		_slvSocket.remove(sid);
		
		// TODO: let the KPFS do file copy
		((KPFSMaster) _kpfsMaster).removeFileInSlave(sid);
		
		for (Task task: _tasks.values()) {
			task.reset();
			for (JobInfo job: task._mapJobs.values()) {
				job._sid = getFreeSlave();
			}
			
			task._phase = Task.TaskPhase.MAP;
			
			System.out.println("\tReschedule " + task._taskName);
		}
		
		System.out.println("ATTENTION: slave " + sid + " is down! Reschedule all tasks after 5s ...");
		
		for (int i=5; i>=1; --i) {
			System.out.println(i+"...");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("Rescheduling...");
		for (Task task: _tasks.values()) {
			JobManager.sharedJobManager().sendJobs(task._mapJobs.values());
		}
	}
}
