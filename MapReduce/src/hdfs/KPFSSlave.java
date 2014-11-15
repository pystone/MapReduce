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

/**
 * @author PY
 *
 */
public class KPFSSlave implements KPFSSlaveInterface {
    public KPFSSlave() {}
	
	@Override
	public String getFileString(String relPath) throws KPFSException {
		File file = new File(GlobalInfo.sharedInfo().getLocalRootDir() + relPath);
		Scanner exp = null;
		try {
			exp = new Scanner(file);
		} catch (FileNotFoundException e) {
			throw new KPFSException("File " + relPath + " is not found!");
		}
		
		String str = "";
		while (exp.hasNextLine()) {
			str += exp.nextLine() + '\n';
		}
		exp.close();
		return str;
	}

	@Override
	public byte[] getFileBytes(String relPath) throws KPFSException {
		File file = new File(GlobalInfo.sharedInfo().getLocalRootDir() + relPath); 
		byte[] byteArr = new byte[(int)file.length()];
		FileInputStream fin = null;
		try {
			fin = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new KPFSException("File " + relPath + " is not found!");
		}
		
		BufferedInputStream bin = new BufferedInputStream(fin);
		
		try {
			bin.read(byteArr, 0, byteArr.length);
			bin.close();
			fin.close();
		} catch (IOException e) {
			throw new KPFSException("Internal error occurs when reading the file " + relPath);
		}
		
		return byteArr;
	}
	
	@Override
	public void storeFile(String relPath, byte[] content) throws KPFSException {
		File file = new File(GlobalInfo.sharedInfo().getLocalRootDir() + relPath); 
		file.getParentFile().mkdirs();
		
		FileOutputStream outStream = null;
		try {
			outStream = new FileOutputStream(file);
			outStream.write(content);
			outStream.close();
		} catch (IOException e) {
			throw new KPFSException("Internal error occurs when storing the file " + relPath);
		}
	}

	

}
