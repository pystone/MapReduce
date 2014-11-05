/**
 * 
 */
package hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * @author PY
 *
 */
public class HDFile {
	String _filePath;
	
	private File _file;
	private Scanner _scan;
	
	HDFile(String filePath) throws FileNotFoundException {
		_filePath = filePath;
		_file = new File(filePath); 
		_scan = new Scanner(_file);
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
}
