/**
 * 
 */
package jobcontrol;

import hdfs.KPFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import mapreduce.GlobalInfo;
import mapreduce.MRBase;
import mapreduce.Pair;
import mapreduce.PairContainer;

/**
 * @author PY
 * 
 */
public class JobInfo implements Serializable {
	private static final long serialVersionUID = 5710312452396530832L;

	public enum JobType {
		NONE, MAP, REDUCE
	};

	public int _jobId = 0;
	public int _taskId = 0;
	public int _sid = 0;
	public JobType _type = JobInfo.JobType.NONE;
	public KPFile _mrFile = null;
	public ArrayList<KPFile> _inputFile = new ArrayList<KPFile>();
	public ArrayList<KPFile> _outputFile = new ArrayList<KPFile>();
	public String _taskName = "";
	
	public HashMap<String, KPFile> interMap = new HashMap<String, KPFile>();
	public HashMap<String, KPFile> resultMap = new HashMap<String, KPFile>();

	public JobInfo(int jobId, String taskName) {
		_jobId = jobId;
		_taskName = taskName;
	}

	public MRBase getMRInstance() throws RemoteException {
		byte[] jarByte = _mrFile.getFileBytes();
		MRBase mrins = null;
		try {
//			File file = File.createTempFile(_taskName, null);
//			file.deleteOnExit();
			FileOutputStream bout = new FileOutputStream(_taskName);
			bout.write(jarByte);
			bout.close();
			
			File file = new File(_taskName);
			
			URL url = file.toURL();  
			URL[] urls = new URL[]{url};
			ClassLoader cl = new URLClassLoader(urls);	
			Class cls = cl.loadClass(_taskName);

			Constructor mapConstr = cls.getConstructor();
			mrins = (MRBase) mapConstr.newInstance();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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

		return mrins;

	}

	public PairContainer getInterPairs() throws RemoteException {
		if (_type != JobInfo.JobType.REDUCE) {
			System.out
					.println("WARNING: try to get intermediate pair for map job!");
			return null;
		}

		PairContainer ret = new PairContainer();

		for (KPFile kpfile : _inputFile) {
			String fileStr = kpfile.getFileString();
			if(fileStr.contains("\n")) {
				String[] parts = fileStr.split("\n");
				for(String part : parts) {
					Pair pair = new Pair(part);
					ret.emit(pair);
				}
			}
		}
		return ret;
	}

	public void saveInterFile(PairContainer interFile, String localHost) {
		if (_type != JobInfo.JobType.MAP) {
			System.out
					.println("WARNING: try to save intermediate pair for reduce job!");
			return;
		}
		
		// encode intermediate pairs into a string
		try {
			Iterator<Pair> itor = interFile.getInitialIterator();
			while (itor.hasNext()) {
				Pair pair = itor.next();
				int hash = (pair.getFirst().hashCode() & Integer.MAX_VALUE)
						% GlobalInfo.sharedInfo().NumberOfReducer;
		
				String interDir = GlobalInfo.sharedInfo().IntermediateDirName;
				String fileName = _taskName + ".inter"
						+ String.format("%03d", hash);
				
				String path = interDir + "/" + fileName;
				KPFile kpfile = null;
				if(interMap.containsKey(path)) {
					kpfile = interMap.get(path);
				} else {
					kpfile = new KPFile(interDir, fileName);
					_outputFile.add(kpfile);
					interMap.put(path, kpfile);
				}
				
				kpfile.saveFileLocally(pair.toString().getBytes(), localHost);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void saveResultFile(PairContainer resultFile, String localHost) {
		if (_type != JobInfo.JobType.REDUCE) {
			System.out.println("WARNING: try to save result pair for map job!");
			return;
		}

		// encode result pairs into a string
		try {
			Iterator<Pair> itor = resultFile.getInitialIterator();
//			int count = 0;
			while (itor.hasNext()) {
				Pair pair = itor.next();
				String interDir = GlobalInfo.sharedInfo().ResultDirName;
//				String fileName = _taskName + ".result"
//						+ String.format("%03d", count++);
				String fileName = _taskName + ".result";

				String path = interDir + "/" + fileName;
				KPFile kpfile = null;
				if(resultMap.containsKey(path)) {
					kpfile = resultMap.get(path);
				} else {
					kpfile = new KPFile(interDir, fileName);
					_outputFile.add(kpfile);
					resultMap.put(path, kpfile);
				}
				
				kpfile.saveFileLocally(pair.toString().getBytes(), localHost);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
