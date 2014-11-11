/**
 * 
 */
package hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;

import mapreduce.GlobalInfo;
import network.Message;

/**
 * @author PY
 *
 */
public class KPFile {
	public String _relDir = null;	// "taskName/*Files/"
	public String _fileName = null;	// "taskName.part001"
	
	private File _file = null;
	private Scanner _scan = null;
	
	
	public void open() throws FileNotFoundException, KPFSException {
		if (_relDir == null || _fileName == null) {
			System.out.println("The path or file name is not set!");
			throw new KPFSException("The path or file name is not set!");
		} else {
			_file = new File(getAbsPath()); 
			_scan = new Scanner(_file);
		}
	}
	
	public String getRelPath() {
		return _relDir + "/" + _fileName;
	}
	
	public String getAbsPath() {
		return GlobalInfo.sharedInfo().FileRootDir + getRelPath();
	}
//
//	public boolean hasNext() {
//		return _scan.hasNext();
//	}
//	
//	public boolean hasNextLine() {
//		return _scan.hasNextLine();
//	}
//	
//	public String next() {
//		return _scan.next();
//	}
//	
//	public String nextLine() {
//		return _scan.nextLine();
//	}
	
	public byte[] getByte() throws IOException {
		byte[] byteArr = new byte[(int)_file.length()];
		FileInputStream fin = new FileInputStream(_file);
		BufferedInputStream bin = new BufferedInputStream(fin);
		bin.read(byteArr, 0, byteArr.length);
		bin.close();
		fin.close();
		return byteArr;
	}
	
	public String getString() throws FileNotFoundException {
		Scanner exp = new Scanner(_file);
		String str = "";
		while (exp.hasNextLine()) {
			str += exp.nextLine() + '\n';
		}
		exp.close();
		return str;
	}
	
	public void saveFileLocally(byte[] byteArr) throws IOException {
		FileOutputStream outStream = new FileOutputStream(_file);
		outStream.write(byteArr);
		outStream.close();
	}
	
//	public void close() {
//		_scan.close();
//	}
	
	
//	
//	/*
//	 * WARNING: this method will overwrite the current file in the path _filePath!
//	 */
//	public void restoreFromString(String path, String content) throws IOException {
//		if (_file != null && _scan != null) {
//			_scan.close();
//		}
//		_filePath = path;
//		_file = new File(_filePath);
//		FileOutputStream outStream = new FileOutputStream(_file);
//		outStream.write(content.getBytes());
//		outStream.close();
//		
//		_scan = new Scanner(_file);
//		_isLocal = true;
//	}
}
