/**
 * 
 */
package mapreduce;

import java.io.Serializable;
import java.net.InetAddress;

import example.Maximum;

/**
 * @author yinxu Base class for mapreduce task, should be extended to map task
 *         and reduce task
 * 
 */
public class MapReduceTask implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4057027904752161050L;

	private int jobId;
	private String[] inputFileName; // specify input files
	private String outputFileName; // specify output files

	private char status; // specify the task status: done, running
	public static final char DONE = 'd';
	public static final char RUNNING = 'r';
	public static final char ERROR = 'e';

	private int type; // specify the job is a map or a reduce, USELESS NOW!!
	public static final int MAP = 0;
	public static final int REDUCE = 1; // USELESS NOW!!

	// should get from conf file
	@SuppressWarnings("rawtypes")
	private Class mapClass;
	@SuppressWarnings("rawtypes")
	private Class mapInputKeyClass;
	@SuppressWarnings("rawtypes")
	private Class mapInputValueClass;
	@SuppressWarnings({ "rawtypes", "unused" })
	private Class mapOutputKeyClass;
	@SuppressWarnings({ "rawtypes", "unused" })
	private Class mapOutputValueClass;

	@SuppressWarnings("rawtypes")
	private Class reduceClass;
	@SuppressWarnings("rawtypes")
	private Class reduceInputKeyClass;
	@SuppressWarnings({ "rawtypes", "unused" })
	private Class reduceInputValueClass;
	@SuppressWarnings({ "unused", "rawtypes" })
	private Class reduceOutputKeyClass;
	@SuppressWarnings({ "rawtypes", "unused" })
	private Class reduceOutputValueClass;

	private InetAddress target;

	public MapReduceTask() {
	}

	public MapReduceTask(String[] inputFileName, char status) {
		this.inputFileName = inputFileName;
		this.status = status;
	}

	public String[] getInputFileName() {
		return inputFileName;
	}

	public void setInputFileName(String[] strings) {
		this.inputFileName = strings;
	}

	public String getOutputFileName() {
		return outputFileName;
	}

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	}

	public char getStatus() {
		return status;
	}

	public void setStatus(char status) {
		this.status = status;
	}

	public InetAddress getTarget() {
		return target;
	}

	public void setTarget(InetAddress target) {
		this.target = target;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public void setConf(MapReduceConf conf) {
		
		this.mapClass = conf.getMapClass();
		this.reduceClass = conf.getReduceClass();
		
		this.mapInputKeyClass = conf.getMapInputKeyClass();
		this.mapInputValueClass = conf.getMapInputValueClass();
		this.mapOutputKeyClass = conf.getMapOutputKeyClass();
		this.mapOutputValueClass = conf.getMapOutputValueClass();
		
		this.reduceInputKeyClass = conf.getReduceInputKeyClass();
		this.reduceInputValueClass = conf.getReduceInputValueClass();
		this.reduceOutputKeyClass = conf.getReduceOutputKeyClass();
		this.reduceOutputValueClass = conf.getReduceOutputValueClass();
	}
	
	public Class getMapClass() {
		return mapClass;
	}

	public void setMapClass(Class mapClass) {
		this.mapClass = mapClass;
	}

	public Class getMapInputKeyClass() {
		return mapInputKeyClass;
	}

	public void setMapInputKeyClass(Class mapInputKeyClass) {
		this.mapInputKeyClass = mapInputKeyClass;
	}

	public Class getMapInputValueClass() {
		return mapInputValueClass;
	}

	public void setMapInputValueClass(Class mapInputValueClass) {
		this.mapInputValueClass = mapInputValueClass;
	}

	public Class getMapOutputKeyClass() {
		return mapOutputKeyClass;
	}

	public void setMapOutputKeyClass(Class mapOutputKeyClass) {
		this.mapOutputKeyClass = mapOutputKeyClass;
	}

	public Class getMapOutputValueClass() {
		return mapOutputValueClass;
	}

	public void setMapOutputValueClass(Class mapOutputValueClass) {
		this.mapOutputValueClass = mapOutputValueClass;
	}

	public Class getReduceClass() {
		return reduceClass;
	}

	public void setReduceClass(Class reduceClass) {
		this.reduceClass = reduceClass;
	}

	public Class getReduceInputKeyClass() {
		return reduceInputKeyClass;
	}

	public void setReduceInputKeyClass(Class reduceInputKeyClass) {
		this.reduceInputKeyClass = reduceInputKeyClass;
	}

	public Class getReduceInputValueClass() {
		return reduceInputValueClass;
	}

	public void setReduceInputValueClass(Class reduceInputValueClass) {
		this.reduceInputValueClass = reduceInputValueClass;
	}

	public Class getReduceOutputKeyClass() {
		return reduceOutputKeyClass;
	}

	public void setReduceOutputKeyClass(Class reduceOutputKeyClass) {
		this.reduceOutputKeyClass = reduceOutputKeyClass;
	}

	public Class getReduceOutputValueClass() {
		return reduceOutputValueClass;
	}

	public void setReduceOutputValueClass(Class reduceOutputValueClass) {
		this.reduceOutputValueClass = reduceOutputValueClass;
	}

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

}
