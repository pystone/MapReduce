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
		NONE, 
		MAP, 
		REDUCE,
		MAP_COMPLETE,
		REDUCE_COMPLETE
	};

	public int _jobId = 0;
	public String _taskName = "";
	public int _sid = 0;
	public JobType _type = JobInfo.JobType.NONE;
	public KPFile _mrFile = null;
	public ArrayList<KPFile> _inputFile = new ArrayList<KPFile>();
	public ArrayList<KPFile> _outputFile = new ArrayList<KPFile>();
	
	
	public HashMap<String, KPFile> resultMap = new HashMap<String, KPFile>();

	public JobInfo(int jobId, String taskName) {
		_jobId = jobId;
		_taskName = taskName;
	}

	public MRBase getMRInstance() throws RemoteException {
		byte[] jarByte = _mrFile.getFileBytes();
		MRBase mrins = null;
		try {
			/* no need to duplicate the jar file */
			File file = File.createTempFile(_taskName, null);
			file.deleteOnExit();
			FileOutputStream bout = new FileOutputStream(file);
			bout.write(jarByte);
			bout.close();
			
			URL[] urls = new URL[]{file.toURI().toURL()};
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
			ret.restoreFromString(fileStr);
		}
		
		return ret;
	}

	public void saveInterFile(PairContainer interFile) {
		if (_type != JobInfo.JobType.MAP) {
			System.out
					.println("WARNING: try to save intermediate pair for reduce job!");
			return;
		}
		
		Iterator<Pair> itor = interFile.getInitialIterator();
		HashMap<Integer, PairContainer> interPairs = new HashMap<Integer, PairContainer>();
		while (itor.hasNext()) {
			Pair pair = itor.next();
			int hash = (pair.getFirst().hashCode() & Integer.MAX_VALUE)
					% GlobalInfo.sharedInfo().NumberOfReducer;
			if (interPairs.get(hash) == null) {
				interPairs.put(hash, new PairContainer());
			}
			PairContainer container = interPairs.get(hash);
			container.emit(pair);
		}
		
		for (Integer hash: interPairs.keySet()) {
			PairContainer container = interPairs.get(hash);
			container.mergeSameKey();
			
			String interDir = _taskName + "/" + GlobalInfo.sharedInfo().IntermediateDirName + "/";
			String fileName = _taskName + _jobId + ".inter"
					+ String.format("%03d", hash);
			
			KPFile kpfile = new KPFile(interDir, fileName);
			try {
				kpfile.saveFileLocally(container.toString().getBytes());
			} catch (IOException e) {
				System.err.println("ERROR: failed to notify metadata of inter file to master!");
				e.printStackTrace();
			}
			_outputFile.add(kpfile);
		}
	}

	public void saveResultFile(PairContainer resultContainer) {
		if (_type != JobInfo.JobType.REDUCE) {
			System.out.println("WARNING: try to save result pair for map job!");
			return;
		}
		
		String interDir = _taskName + "/" + GlobalInfo.sharedInfo().ResultDirName + "/";
		String fileName = _taskName + ".result" + _jobId;
		
		KPFile kpfile = new KPFile(interDir, fileName);
		try {
			kpfile.saveFileLocally(resultContainer.toString().getBytes());
		} catch (IOException e) {
			System.err.println("ERROR: failed to notify metadata of result file to master!");
			e.printStackTrace();
		}
		_outputFile.add(kpfile);

	}
	
	public HashMap<Integer, KPFile> getInterFilesWithIndex() {
		if (_type != JobInfo.JobType.MAP_COMPLETE || _outputFile.isEmpty()) {
			System.out.println("WARNING: try to get all the indeces for non completed map job!");
			return null;
		}
		HashMap<Integer, KPFile> ret = new HashMap<Integer, KPFile>();
		for  (KPFile kp: _outputFile) {
			String idxStr = kp._fileName.substring(kp._fileName.length() - 3);
			ret.put(Integer.parseInt(idxStr), kp);
		}
		return ret;
	}
	
	public void serialize() {
		System.out.println("TaskName: " + _taskName);
		System.out.println("JobID: " + _jobId);
		System.out.println("Type: " + _type.toString());
		System.out.println("Output Files: ");
		for (KPFile file: _outputFile) {
			System.out.println("\t" + file.getRelPath());
		}
	}
}
