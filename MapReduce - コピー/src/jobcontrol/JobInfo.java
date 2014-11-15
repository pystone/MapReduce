/**
 * 
 */
package jobcontrol;

import hdfs.KPFSException;
import hdfs.KPFile;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Iterator;

import mapreduce.GlobalInfo;
import mapreduce.MRBase;
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
		REDUCE
	};
	public int _jobId = 0;
	public int _taskId = 0;
	public int _sid = 0;
	public JobType _type = JobInfo.JobType.NONE;
	public KPFile _mrFile = null;
	public KPFile _inputFile = null;
	public KPFile _outputFile = null;
	public String _taskName = "";
	
	
	public JobInfo(int jobId, String taskName) {
		_jobId = jobId;
		_taskName = taskName;
	}
	
	public MRBase getMRInstance() {
		byte[] jarByte = _mrFile.getFileBytes();
		MRBase mrins = null;
		try {
			
			File file = File.createTempFile(_taskName, null);
			file.deleteOnExit();
			FileOutputStream bout = new FileOutputStream(file);
			bout.write(jarByte);
			bout.close();
			
			URL[] urls = {file.toURI().toURL()};
			Class cls = (new URLClassLoader(urls)).loadClass(_taskName);
			
			Constructor mapConstr = cls.getConstructor();
			mrins = (MRBase)mapConstr.newInstance();
			
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
	
	public PairContainer getInterPairs() {
		if (_type != JobInfo.JobType.REDUCE) {
			System.out.println("WARNING: try to get intermediate pair for map job!");
			return null;
		}
		
		PairContainer ret = new PairContainer();
		String fileStr = _inputFile.getFileString();
		// TODO: parse intermediate pairs from input file into ret
		
		return ret;
	}
	
	public void saveInterFile(PairContainer interFile, String localHost) {
		if (_type != JobInfo.JobType.MAP) {
			System.out.println("WARNING: try to save intermediate pair for reduce job!");
			return;
		}
		
		String toSaveStr = ""; // TODO: encode intermediate pairs into a string
		
		try {
			_outputFile.saveFileLocally(toSaveStr.getBytes(), localHost);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void saveResultFile(PairContainer resultFile, String localHost) {
		if (_type != JobInfo.JobType.REDUCE) {
			System.out.println("WARNING: try to save result pair for map job!");
			return;
		}
		
		String toSaveStr = ""; // TODO: encode result pairs into a string
		
		try {
			_outputFile.saveFileLocally(toSaveStr.getBytes(), localHost);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
