/**
 * 
 */
package mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import dfs.MasterServer;
import dfs.SlaveInfo;
import dfs.YZFS;
import example.Maximum;

import mapreduce.OutputCollector.Entry;
import message.*;

/**
 * @author yinxu
 * 
 */
public class MapReduceMasterThread extends Thread {

	private ObjectInputStream input;
	private ObjectOutputStream output;

	public MapReduceMasterThread(Socket sock) {

		try {
			input = new ObjectInputStream(sock.getInputStream());
			output = new ObjectOutputStream(sock.getOutputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void run() {

		Object msg = null;
		try {
			msg = (Object) input.readObject();
			AckMsg ack = new AckMsg(true);
			output.writeObject(ack);
			output.flush();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (msg instanceof NewJobMsg) {
			executeNewJob((NewJobMsg) msg);
		} else if (msg instanceof MapReduceTask) {
			ackTaskFinish((MapReduceTask) msg);
		}
	}

	public void executeNewJob(NewJobMsg msg) {
		String jobName = msg.getJobName(); // For future use
		System.out.println("enterred executeNewJob: " + jobName);

		int jobId = MasterServer.jobId.incrementAndGet();

		synchronized (MasterServer.jobToTaskCount) {
			MasterServer.jobToTaskCount.put(jobId, 0);
		}

		/* split the job and generate list of tasks */
		// Get the input file names (socket to YZFS)
		try {

			// generate task list
			generateTaskList(jobName, jobId);
			System.out.println("new job:" + jobName + "has been added");

			/* send out tasks */
			sendOutTasks();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/* generate mapper task list */
	public void generateTaskList(String jobName, int jobId) throws ClassNotFoundException,
			SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {

		for (ArrayList<String> partList : MasterServer.fileToPart.values()) {
			for (String part : partList) {
				MapReduceTask task = new MapReduceTask();
				task.setInputFileName(new String[] { part });
				task.setOutputFileName(part + ".output");
				
				// always choose the first candidate
				InetAddress target = MasterServer.partToSlave.get(part).get(0).iaddr;

				task.setTarget(target);
				task.setType(MapReduceTask.MAP);
				task.setJobId(jobId);

				Class<?> jobClass = Class.forName("example." + jobName);
				Method getConfMethod = jobClass.getMethod("getMapReduceConf", null);
				MapReduceConf conf = (MapReduceConf) getConfMethod.invoke(null, null);
				
				task.setConf(conf);

				synchronized (MasterServer.mapQueue) {
					MasterServer.mapQueue.add(task);
				}
				synchronized (MasterServer.jobToTaskCount) {
					MasterServer.jobToTaskCount.put(jobId,
							MasterServer.jobToTaskCount.get(jobId) + 1);
				}
			}
		}
	}

	/* send out the MapReduceTask in the map queue */
	public void sendOutTasks() {

		System.out.println("sending out map tasks...");
		Socket sockTask;
		ObjectOutputStream outputTask;
		int taskCount = 0;

		while (!MasterServer.mapQueue.isEmpty()) {

			MapReduceTask task;

			synchronized (MasterServer.mapQueue) {
				task = MasterServer.mapQueue.remove();
			}

			try {
				sockTask = new Socket(task.getTarget(), YZFS.MP_PORT);
				outputTask = new ObjectOutputStream(sockTask.getOutputStream());
				task.setStatus(MapReduceTask.RUNNING);
				outputTask.writeObject(task);
				outputTask.flush();

				System.out.println("sent task " + taskCount++);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	/* called when receive task finish message */
	public void ackTaskFinish(MapReduceTask task) {
		System.out.println("Finished task: " + task.getInputFileName()[0]);
		int jobCount = 0;
		if (task.getStatus() != MapReduceTask.ERROR) {

			synchronized (MasterServer.jobToTaskCount) {
				jobCount = MasterServer.jobToTaskCount.get(task.getJobId());
				System.err.println(jobCount);
				if (jobCount == 1) {
					System.out.println("All mapper tasks finshed.");
					// indicating all mappers are done
					System.out.println("starting reducer...");
					try {
						setReduceInputFile(task);
						reduce(task);
						System.out.println("DONE!!!!! jobID: " + task.getJobId());
						
					} catch (Throwable e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {
					// jobCount-1
					MasterServer.jobToTaskCount.put(task.getJobId(), jobCount - 1);
				}
			}

		} else {
			System.out.println("Error happened");
		}

	}

	@SuppressWarnings({ "unused", "rawtypes", "unchecked" })
	private void reduce(MapReduceTask task) throws Throwable {

		OutputCollector reduceOutput = new OutputCollector();
		Reporter reporter = new Reporter();

		// instantiate a reducer
		Constructor reduceConstr = task.getReduceClass().getConstructor(null);
		Object reducer = reduceConstr.newInstance(null);

		// get a reduce method from the reducer
		Class<?>[] reduceMethodClassArgs = { task.getReduceInputKeyClass(), Iterator.class,
				OutputCollector.class, Reporter.class };
		Method reduceMethod = task.getReduceClass().getMethod("reduce", reduceMethodClassArgs);

		// read reduce inputs obj from map result obj
		int size = task.getInputFileName().length;
		OutputCollector[] reduceInputs = new OutputCollector[size];
		Entry[] entries = new Entry[size];

		FileInputStream fileIn = null;
		ObjectInputStream objIn = null;
		for (int i = 0; i < size; i++) {
			System.out.println(task.getInputFileName()[i]);
			// /
			fileIn = new FileInputStream(task.getInputFileName()[i]);
			objIn = new ObjectInputStream(fileIn);
			reduceInputs[i] = ((OutputCollector) objIn.readObject());
			entries[i] = (Entry) reduceInputs[i].queue.poll();
		}

		// get getHashcode method from key obj
		Object key = entries[0].getKey();
		Method getHashcode = key.getClass().getMethod("getHashcode", null);
		ArrayList<Integer> minIndices = null;

		// start the merge sort
		while ((minIndices = getMinIndices(entries, getHashcode)) != null) {
			key = entries[minIndices.get(0)].getKey();
			ArrayList values = new ArrayList();
			Iterator itrValues = null;

			// add every value (that has the least key hash value) into the
			// value list
			for (int i : minIndices) {
				values.add(entries[i].getValue());
				entries[i] = (Entry) reduceInputs[i].queue.poll();
			}

			// invoke reduce method
			itrValues = values.iterator();
			Object[] reduceMethodObjectArgs = { key, itrValues, reduceOutput, reporter };
			reduceMethod.invoke(reducer, reduceMethodObjectArgs);

		}

		File file = new File("/tmp/YZFS/output.txt");
		FileWriter fileWriter = new FileWriter(file);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

		while (reduceOutput.queue.size() != 0) {
			bufferedWriter.write(reduceOutput.queue.poll().toString() + "\n");
		}
		bufferedWriter.close();

	}

	private ArrayList<Integer> getMinIndices(Entry[] entries, Method getHashcode) throws Throwable,
			NoSuchMethodException {
		ArrayList<Integer> ret = null;
		int length = entries.length;
		int minHash = Integer.MAX_VALUE;

		for (int i = 0; i < length; i++) {
			if (entries[i] == null)
				continue;

			int hash = ((Integer) getHashcode.invoke(entries[i].getKey(), null));
			if (hash < minHash) {
				minHash = hash;
				ret = new ArrayList<Integer>();
				ret.add(i);
			} else if (hash == minHash) {
				ret.add(i);
			}
		}

		return ret;
	}

	public void setReduceInputFile(MapReduceTask task) {
		File[] files = new File(YZFS.fileSystemWorkingDir).listFiles();
		String[] inputFiles = new String[files.length];
		for (int i = 0; i < inputFiles.length; i++) {
			inputFiles[i] = files[i].toString();
		}
		task.setInputFileName(inputFiles);
	}
	
	
	

}
