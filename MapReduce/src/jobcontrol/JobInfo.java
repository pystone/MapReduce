/**
 * 
 */
package jobcontrol;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Iterator;

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
	public String _inFilePath = "";
	public String _interFileDir = "";
	public String _outFileDir = "";
	public String _mapperPath = "";
	public String _reducerPath = "";
	public String _mrPath = "";
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
	
	public String getInFileName() {
		// TODO
		return "";
	}
	
	public String getFileContent() {
		// TODO
		return "";
	}
	
	public String getInterFileName() {
		// TODO
		return "";
	}
	
	public String getResultFileName() {
		// TODO
		return "";
	}
	
	public void saveInterFile(PairContainer<String, Iterator<String>> interFile) {
		// TODO
	}
	
	public void saveResultFile(PairContainer<String, String> resultFile) {
		// TODO
	}
}
