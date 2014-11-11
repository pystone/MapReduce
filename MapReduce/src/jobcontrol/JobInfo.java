/**
 * 
 */
package jobcontrol;

import hdfs.KPFSException;
import hdfs.KPFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
		Class<?> mrclass;
		try {
			mrclass = Class.forName("example." + _taskName);
			Constructor<?> ctor = mrclass.getConstructor(String.class);
			Object object = ctor.newInstance();
			MRBase mrins = (MRBase) object;
			return mrins;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
		
	}
	
	public PairContainer getInterPairs() {
		if (_type != JobInfo.JobType.REDUCE) {
			System.out.println("WARNING: try to get intermediate pair for map job!");
			return null;
		}
		
		PairContainer ret = new PairContainer();
		try {
			String instr = _inputFile.getString();
			// TODO: parse intermediate pairs from input file into ret
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		return ret;
	}
	
	public void saveInterFile(PairContainer interFile) {
		if (_type != JobInfo.JobType.MAP) {
			System.out.println("WARNING: try to save intermediate pair for reduce job!");
			return;
		}
		
		String toSaveStr = ""; // TODO: encode intermediate pairs into a string
		
		try {
			_outputFile.saveFileLocally(toSaveStr.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void saveResultFile(PairContainer resultFile) {
		if (_type != JobInfo.JobType.REDUCE) {
			System.out.println("WARNING: try to save result pair for map job!");
			return;
		}
		
		String toSaveStr = ""; // TODO: encode result pairs into a string
		
		try {
			_outputFile.saveFileLocally(toSaveStr.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
