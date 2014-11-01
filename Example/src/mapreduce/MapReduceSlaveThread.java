/**
 * 
 */
package mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import dfs.CommunicationModule;
import dfs.YZFS;

import example.Maximum;

import mapreduce.OutputCollector.Entry;
import message.AckMsg;
import message.DownloadFileMsg;
import message.Message;

/**
 * @author yinxu This class is in charge of running the received MapReduceTask
 * 
 */
public class MapReduceSlaveThread extends Thread {

	private MapReduceTask task;
	private Socket sock;
	private ObjectInputStream input;
	private ObjectOutputStream output;
	private ServerSocket dwldSocket;
	
	private InetAddress masterIP;
	private int masterPort;

	public MapReduceSlaveThread(Socket sock, ServerSocket serverSocket) throws FileNotFoundException, IOException {
		
		// load a properties file to read master ip and port
		Properties prop = new Properties();
		prop.load(new FileInputStream(YZFS.fileSystemWorkingDir + ".masterinfo.config"));
		this.masterIP = InetAddress.getByName(prop.getProperty("master host name"));
		this.masterPort = Integer.parseInt(prop.getProperty("master port number"));
		
		this.sock = sock;
		this.dwldSocket = serverSocket;
		
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

		
		Object obj;
		MapReduceTask task = null;
		try {
			obj = input.readObject();
			System.out.println("received a task object");
			if (obj instanceof MapReduceTask) {
				task = (MapReduceTask) obj;
			} else {
				System.out.println("invalid object received");
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		// if the task is a map task perform mapper
		if (task.getType() == MapReduceTask.MAP) {
			try {
				map(task);
				
				System.out.println("one map task finished");
				//// get prepared to upload the intermediate result
				MapReduceDownloadThread mpDldThread = new MapReduceDownloadThread(task, dwldSocket);
				mpDldThread.start();
				
				// send download msg to file server
				DownloadFileMsg dfmsg = new DownloadFileMsg(InetAddress.getLocalHost(), YZFS.MP_DOWNLOAD_PORT, task.getJobId());
				dfmsg.setTask(task);
				dfmsg.setFileFullPath(YZFS.fileSystemWorkingDir + task.getOutputFileName());
				Socket sockFS = new Socket(this.masterIP, this.masterPort);
				System.out.println("sending downloading request");
				
				////
				ObjectOutputStream objOutput = new ObjectOutputStream(sockFS.getOutputStream());
				objOutput.writeObject(dfmsg);
				objOutput.flush();
				System.out.println("sent out");
				
				ObjectInputStream objInput = new ObjectInputStream(sockFS.getInputStream());
				DownloadFileMsg reply = (DownloadFileMsg) objInput.readObject();
				
				
				System.out.println("downloading msg received");
				
				if (reply.isSuccessful()) {
					System.out.println("sent download request to master");
				}

				
			} catch (Exception e) {
				// map task failed
				task.setStatus(MapReduceTask.ERROR);
				e.printStackTrace();
			}
			
		} else if (task.getType() == MapReduceTask.REDUCE) { //??? USELESS code!!!
			try {
				//reduce(task);
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.err.println("Wrong Map Reduce Task Type!!!");
		}


	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	private void map(MapReduceTask task) throws ClassNotFoundException, SecurityException,
			NoSuchMethodException, IllegalArgumentException, InstantiationException,
			IllegalAccessException, InvocationTargetException, IOException {

		OutputCollector mapOutput = new OutputCollector();
		OutputCollector combineOutput = new OutputCollector();
		Reporter reporter = new Reporter();

		// instantiate a mapper
		Constructor mapConstr = task.getMapClass().getConstructor(null);
		Object mapper = mapConstr.newInstance(null);

		// get a map method from the mapper
		Class<?>[] mapMethodClassArgs = {task.getMapInputKeyClass(), task.getMapInputValueClass(),
				OutputCollector.class, Reporter.class};
		Method mapMethod = task.getMapClass().getMethod("map", mapMethodClassArgs);

		// instatiate a inputValue
		Constructor inputValueConstr = task.getMapInputValueClass().getConstructor(null);
		Object inputValue = inputValueConstr.newInstance(null);

		// get a set method from the inputValue
		Method setInputValue = task.getMapInputValueClass().getMethod("set",
				new Class<?>[]{String.class});

		// invoke map method for every line of the input file
		BufferedReader bufferedReader = new BufferedReader(new FileReader(
				YZFS.fileSystemWorkingDir + task.getInputFileName()[0]));
		String line;
		int i = 0;
		while ((line = bufferedReader.readLine()) != null) {
			setInputValue.invoke(inputValue, line);
			Object[] mapMethodObjectArgs = {null, inputValue, mapOutput, reporter};
			mapMethod.invoke(mapper, mapMethodObjectArgs);
			System.out.println("I am still runing...." + i++);
		}

		// print out the result of map
		// System.out.println("debug");

		// instantiate a combiner
		Constructor combineConstr = task.getReduceClass().getConstructor(null);
		Object combiner = combineConstr.newInstance(null);

		// get a combine method from the combiner
		Class<?>[] combineMethodClassArgs = {task.getReduceInputKeyClass(), Iterator.class,
				OutputCollector.class, Reporter.class};
		Method combineMethod = task.getReduceClass().getMethod("reduce", combineMethodClassArgs);

		// start combining the map result
		mapreduce.OutputCollector.Entry entry = (Entry) mapOutput.queue.poll();
		mapreduce.OutputCollector.Entry tmpEntry = null;
		ArrayList values = new ArrayList();
		Iterator itrValues = null;

		Object key = entry.getKey();
		values.add(entry.getValue());

		Method method = key.getClass().getMethod("getHashcode", null);
		int hash = ((Integer) method.invoke(key, null));
		int tmpHash = 0;

		// invoke reduce method for every key-value pair that has the same key
		// hashcode
		while (mapOutput.queue.size() != 0) {
			tmpEntry = (Entry) mapOutput.queue.poll();
			tmpHash = ((Integer) method.invoke(tmpEntry.getKey(), null));
			if (tmpHash == hash) {
				values.add(tmpEntry.getValue());
			} else {
				itrValues = values.iterator();
				Object[] combineMethodObjectArgs = {key, itrValues, combineOutput, reporter};
				combineMethod.invoke(combiner, combineMethodObjectArgs);
				entry = tmpEntry;
				key = entry.getKey();
				hash = ((Integer) method.invoke(key, null));
				values = new ArrayList();
				values.add(entry.getValue());
			}
		}

		// don't forget the last one :)
		itrValues = values.iterator();
		Object[] combineMethodObjectArgs = {key, itrValues, combineOutput, reporter};
		combineMethod.invoke(combiner, combineMethodObjectArgs);

		// write the combine result into object file
		FileOutputStream fileOut = new FileOutputStream(YZFS.fileSystemWorkingDir + task.getOutputFileName());
		ObjectOutputStream objOut = new ObjectOutputStream(fileOut);
		objOut.writeObject(combineOutput);

		// while (combineOutput.queue.size() != 0)
		// System.out.print(combineOutput.queue.poll() + "  ");
	}

//	@SuppressWarnings({"unused", "rawtypes", "unchecked"})
//	private void reduce(MapReduceTask task) throws Throwable {
//
//		OutputCollector reduceOutput = new OutputCollector();
//		Reporter reporter = new Reporter();
//
//		// instantiate a reducer
//		Constructor reduceConstr = task.getReduceClass().getConstructor(null);
//		Object reducer = reduceConstr.newInstance(null);
//
//		// get a reduce method from the reducer
//		Class<?>[] reduceMethodClassArgs = {task.getReduceInputKeyClass(), Iterator.class,
//				OutputCollector.class, Reporter.class};
//		Method reduceMethod = task.getReduceClass().getMethod("reduce", reduceMethodClassArgs);
//
//		// read reduce inputs obj from map result obj
//		int size = task.getInputFileName().length;
//		OutputCollector[] reduceInputs = new OutputCollector[size];
//		Entry[] entries = new Entry[size];
//
//		FileInputStream fileIn = null;
//		ObjectInputStream objIn = null;
//		for (int i = 0; i < size; i++) {
//			fileIn = new FileInputStream(task.getInputFileName()[i]);
//			objIn = new ObjectInputStream(fileIn);
//			reduceInputs[i] = ((OutputCollector) objIn.readObject());
//			entries[i] = (Entry) reduceInputs[i].queue.poll();
//		}
//
//		// get getHashcode method from key obj
//		Object key = entries[0].getKey();
//		Method getHashcode = key.getClass().getMethod("getHashcode", null);
//		ArrayList<Integer> minIndices = null;
//
//		// start the merge sort
//		while ((minIndices = getMinIndices(entries, getHashcode)) != null) {
//			key = entries[minIndices.get(0)].getKey();
//			ArrayList values = new ArrayList();
//			Iterator itrValues = null;
//
//			// add every value (that has the least key hash value) into the
//			// value list
//			for (int i : minIndices) {
//				values.add(entries[i].getValue());
//				entries[i] = (Entry) reduceInputs[i].queue.poll();
//			}
//
//			// invoke reduce method
//			itrValues = values.iterator();
//			Object[] reduceMethodObjectArgs = {key, itrValues, reduceOutput, reporter};
//			reduceMethod.invoke(reducer, reduceMethodObjectArgs);
//
//		}
//
//		File file = new File("output.txt");
//		FileWriter fileWriter = new FileWriter(file);
//		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
//
//		while (reduceOutput.queue.size() != 0) {
//			// System.out.print(reduceOutput.queue.poll() + "  ");
//			bufferedWriter.write(reduceOutput.queue.poll().toString() + "\n");
//		}
//		bufferedWriter.close();
//
//	}

//	public ArrayList<Integer> getMinIndices(Entry[] entries, Method getHashcode)
//			throws Throwable, NoSuchMethodException {
//		ArrayList<Integer> ret = null;
//		int length = entries.length;
//		int minHash = Integer.MAX_VALUE;
//
//		for (int i = 0; i < length; i++) {
//			if (entries[i] == null)
//				continue;
//
//			int hash = ((Integer) getHashcode.invoke(entries[i].getKey(), null));
//			if (hash < minHash) {
//				minHash = hash;
//				ret = new ArrayList<Integer>();
//				ret.add(i);
//			} else if (hash == minHash) {
//				ret.add(i);
//			}
//		}
//
//		return ret;
//	}
	
}
