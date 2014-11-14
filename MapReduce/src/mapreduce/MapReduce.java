/**
 * 
 */
package mapreduce;

import hdfs.KPFSMaster;
import hdfs.KPFileSplit;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;
import java.util.StringTokenizer;

/**
 * @author PY
 *
 */
public class MapReduce {

	private static int _masterPort;
	private static int _slavePort;
	private static String _masterHost;
	private static String[] _slaveHosts;
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Please provide config file name!");
			System.exit(-1);
		}
//		start(args[0], args[1]);
		
		
		
	}
	
	private static void start(String confFileName, String mstOrSlv) {
		Properties prop = new Properties();
		try {
			InputStream propInput = new FileInputStream(confFileName);
			prop.load(propInput);
		} catch (FileNotFoundException e) {
			System.out.println("Config file " + confFileName + " not found!");
			System.exit(-1);
		} catch (IOException e) {
			System.out.println("Invalid config file " + confFileName + ". Please check again.");
			System.exit(-1);
		}
		
		loadConfig(prop);
		
		if (mstOrSlv.equalsIgnoreCase("m") && GlobalInfo.sharedInfo().isMaster()) {
			System.out.println("Master");
			Master.sharedMaster().start();
		} else if (mstOrSlv.equalsIgnoreCase("s") && GlobalInfo.sharedInfo().isSlave()) {
			System.out.println("Slave");
			Slave.sharedSlave().start();
		} else {
			System.out.println("This machine is not included in config file!");
		}
	}
	
	private static void loadConfig(Properties prop) {
		GlobalInfo.sharedInfo().MasterPort = Integer.parseInt(prop.getProperty("MasterPort"));
		GlobalInfo.sharedInfo().SlavePort = Integer.parseInt(prop.getProperty("SlavePort"));
		GlobalInfo.sharedInfo().MasterHost = prop.getProperty("MasterHost");
		
		String[] slaves = prop.getProperty("SlaveHosts").split(",");
		for(int i=0; i<slaves.length; ++i) {
			GlobalInfo.sharedInfo().SID2Host.put(i+1, slaves[i].trim());
		}
		
		String[] slaverootdir = prop.getProperty("SlaveRootDir").split(",");
		for(int i=0; i<slaverootdir.length; ++i) {
			GlobalInfo.sharedInfo().Host2RootDir.put(slaves[i].trim(), slaverootdir[i].trim());
		}
		
		GlobalInfo.sharedInfo().FileChunkSizeB = Integer.parseInt(prop.getProperty("FileChunkSizeB"));
		GlobalInfo.sharedInfo().MasterRootDir = prop.getProperty("MasterRootDir");
		
		GlobalInfo.sharedInfo().IntermediateDirName = prop.getProperty("IntermediateDirName");
		GlobalInfo.sharedInfo().ChunkDirName = prop.getProperty("ChunkDirName");
		GlobalInfo.sharedInfo().ResultDirName = prop.getProperty("ResultDirName");
		
		GlobalInfo.sharedInfo().DataMasterHost = prop.getProperty("DataMasterHost");
		GlobalInfo.sharedInfo().DataMasterPort = Integer.parseInt(prop.getProperty("DataMasterPort"));
		GlobalInfo.sharedInfo().DataSlavePort = Integer.parseInt(prop.getProperty("DataSlavePort"));
		
		_masterPort = Integer.parseInt(prop.getProperty("MasterPort"));
		_slavePort = Integer.parseInt(prop.getProperty("SlavePort"));
		_masterHost = prop.getProperty("MasterHost");
		_slaveHosts = prop.getProperty("SlaveHosts").split(",");
		for (int i=0; i<_slaveHosts.length; ++i) {
			_slaveHosts[i] = _slaveHosts[i].trim();
		}
		
	}
	
	// ====== test begin ======
		private static void testSplit() {
			File file = new File("words.txt");
			String dir = file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf('/') + 1);
			String fileName = file.getAbsolutePath().substring(file.getAbsolutePath().lastIndexOf("/") + 1);
			System.out.println(dir);
			System.out.println(fileName);
			try {
				ArrayList<String> files = KPFileSplit.split(file.getAbsolutePath(), 20, dir, fileName);
				for (String fn: files) {
					System.out.println(fn);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		private static void testMkdir() {
			File aa = new File("/Users/PY/Desktop/pypy");
			if (aa.mkdirs() == true) {
				System.out.println("mkdir successfully!");
			} else {
				System.out.println("failed to mkdir!");
			}
		}
		private static void testKPFSMaster() {
			KPFSMaster master = new KPFSMaster();
			master.addFileLocation("a", "1.1.1.1", 14);
			master.addFileLocation("a", "2.2.2.2", 15);
			master.addFileLocation("b", "1.1.1.1", 16);
			master.addFileLocation("c", "3.3.3.3", 17);
			System.out.println(master.getFileLocation("a"));
			System.out.println(master.getFileLocation("d"));
			System.out.println(master.getFileLocation("c"));
			master.removeFileLocation("a", "1.1.1.1");
			System.out.println(master.getFileLocation("a"));
		}
		
		private static void testLoadJar() {
			File jarFile = new File("jar/WordCounter.jar");
			byte[] byteArr = new byte[(int)jarFile.length()];

			try {
				FileInputStream fin = new FileInputStream(jarFile);
				BufferedInputStream bin = new BufferedInputStream(fin);
				bin.read(byteArr, 0, byteArr.length);
				bin.close();
				fin.close();
				
				File file = File.createTempFile("WordCounter", null);
				file.deleteOnExit();
				FileOutputStream bout = new FileOutputStream(file);
				bout.write(byteArr);
				bout.close();
				
//				File file = new File("jar/WordCounter.jar");
				
				System.out.println(file.getAbsolutePath());
				System.out.println(file.exists());
				
				URL[] urls = {file.toURI().toURL()};
				Class cls = (new URLClassLoader(urls)).loadClass("WordCounter");
				
				Constructor mapConstr = cls.getConstructor();
				MRBase mapper = (MRBase)mapConstr.newInstance();
				mapper.map("xxx", "b", new PairContainer());
				
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
}
