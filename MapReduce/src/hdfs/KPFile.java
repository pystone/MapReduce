/**
 * 
 */
package hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;

import network.Message;

/**
 * @author PY
 *
 */
public class KPFile {
	String _filePath;
	private File _file;
	private Scanner _scan;
	private boolean _isLocal = true;
	
	public KPFile(boolean isLocal) {
		_isLocal = isLocal;
	}
	
	public void open(String filePath) throws FileNotFoundException, KPFSException {
		if (_isLocal == true) {
			_filePath = filePath;
			_file = new File(_filePath); 
			_scan = new Scanner(_file);
		} else {
			System.out.println("This file is not local! Please use `exportToString` to restore from network message");
			throw new KPFSException("This file is not local!");
		}
	}
	
	public boolean hasNext() {
		return _scan.hasNext();
	}
	
	public boolean hasNextLine() {
		return _scan.hasNextLine();
	}
	
	public String next() {
		return _scan.next();
	}
	
	public String nextLine() {
		return _scan.nextLine();
	}
	
	public String exportToString() throws FileNotFoundException {
		Scanner exp = new Scanner(_file);
		String str = "";
		while (exp.hasNextLine()) {
			str += exp.nextLine();
		}
		return str;
	}
	
	/*
	 * WARNING: this method will overwrite the current file in the path _filePath!
	 */
	public void restoreFromString(String path, String content) throws IOException {
		if (_file != null && _scan != null) {
			_scan.close();
		}
		_filePath = path;
		_file = new File(_filePath);
		FileOutputStream outStream = new FileOutputStream(_file);
		outStream.write(content.getBytes());
		outStream.close();
		
		_scan = new Scanner(_file);
		_isLocal = true;
	}
}
