package mapreduce;

public class MapReduceConf {

	private Class mapClass;
	private Class reduceClass;

	private Class mapInputKeyClass;
	private Class mapInputValueClass;
	private Class mapOutputKeyClass;
	private Class mapOutputValueClass;

	private Class reduceInputKeyClass;
	private Class reduceInputValueClass;
	private Class reduceOutputKeyClass;
	private Class reduceOutputValueClass;

	public Class getMapClass() {
		return mapClass;
	}

	public void setMapClass(Class mapClass) {
		this.mapClass = mapClass;
	}

	public Class getReduceClass() {
		return reduceClass;
	}

	public void setReduceClass(Class reduceClass) {
		this.reduceClass = reduceClass;
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

	public void setMapOutputValueClass(Class mapOutputValue) {
		this.mapOutputValueClass = mapOutputValue;
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

}
