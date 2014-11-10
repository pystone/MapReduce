/**
 * 
 */
package jobcontrol;

import hdfs.KPFSException;
import hdfs.KPFile;

import java.io.File;
import java.io.FileNotFoundException;
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
	public String _mrPath = "";
	public String _taskName = "";
	public String _dir = "";
	
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
	
	public String getInputFileName() {
		if (_type == JobInfo.JobType.MAP) {
			return getInFileName();
		} else if (_type == JobInfo.JobType.REDUCE) {
			return getInterFileName();
		}
		return null;
	}
	
	public String getInFileName() {
		return GlobalInfo.sharedInfo().SlaveRootDir + "/" + _taskName + "/" + GlobalInfo.sharedInfo().ChunkDirName + "/" + _taskName + "." + _jobId;
	}
	
	public String getInterFileName() {
		return GlobalInfo.sharedInfo().SlaveRootDir + "/" + _taskName + "/" + GlobalInfo.sharedInfo().IntermediateDirName + "/" + _taskName + "." + _jobId;
	}
	
	public String getResultFileName() {
		return GlobalInfo.sharedInfo().SlaveRootDir + "/" + _taskName + "/" + GlobalInfo.sharedInfo().ResultDirName + "/" + _taskName + "." + _jobId;
	}
	
	public PairContainer getInterPairs() {
		KPFile file = new KPFile(true);
		PairContainer pairs = new PairContainer();
		
		try {
			file.open(getInterFileName());
			
			while (file.hasNextLine()) {
				String line = file.nextLine();
				
			}
		} catch (FileNotFoundException | KPFSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO
		return null;
	}
	
	public void saveInterFile(PairContainer interFile) {
		// TODO
	}
	
	public void saveResultFile(PairContainer resultFile) {
		// TODO
	}
}
