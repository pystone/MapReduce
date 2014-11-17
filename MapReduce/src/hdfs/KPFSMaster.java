/**
 * 
 */
package hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Scanner;

import mapreduce.GlobalInfo;

/**
 * @author PY
 *
 */
public class KPFSMaster implements KPFSMasterInterface {
	public KPFSMaster() {}

	private HashMap<String, ArrayList<KPFSFileInfo>> _mapTbl = new HashMap<String, ArrayList<KPFSFileInfo>>(); 
	
	@Override
	public ArrayList<String> splitFile(String filePath, int chunkSizeB,
			String directory, String fileName) {
		try {
			File file = new File(filePath);
			Scanner scan = new Scanner(file);
			ArrayList<String> smallFiles = new ArrayList<String>();
			String curFile = "";
			String curLine = "";
			int partCnt = 0;

			while (scan.hasNextLine()) {
				for (; scan.hasNextLine() && curFile.length() < chunkSizeB;) {
					curLine = scan.nextLine();
					curFile += curLine + '\n';
				}

				String curFileName = directory + fileName + ".part"
						+ String.format("%03d", partCnt++);
				File outFile = new File(curFileName);
				FileOutputStream outStream = new FileOutputStream(outFile);
				try {
					outStream.write(curFile.getBytes());
					outStream.close();
				} catch (IOException e) {
					System.out.println("Failed to write chunk file!");
					e.printStackTrace();
				}

				smallFiles.add(curFileName);
				addFileLocation(curFileName, 0, outFile.length());	// 0 is the id of master

				/* release the memory */
				curFile = curLine = "";
			}
			return smallFiles;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public KPFSFileInfo getFileLocation(String relPath) {
		ArrayList<KPFSFileInfo> ips = (ArrayList<KPFSFileInfo>) _mapTbl.get(relPath);
		if (ips==null || ips.isEmpty()) {
			return null;
		}
		
		Random rand = new Random();
		int idx = rand.nextInt(ips.size());	// load balancer entry point
		KPFSFileInfo kpfsfileinfo = ips.get(idx);
		return kpfsfileinfo;
	}
	
	@Override
	public boolean addFileLocation(String relPath, int sid, long size) {
		ArrayList<KPFSFileInfo> ips = _mapTbl.get(relPath);
		if (ips == null) {
			ips = new ArrayList<KPFSFileInfo>();
			_mapTbl.put(relPath, ips);
		}
		KPFSFileInfo val = new KPFSFileInfo(sid, size);
		ips.add(val);
		return true;
	}
	
	@Override
	public void removeFileLocation(String relPath, int sid) {
		ArrayList<KPFSFileInfo> ips = _mapTbl.get(relPath);
		if (ips == null) {
			return;
		}
		Iterator<KPFSFileInfo> iter = ips.iterator();
		if (iter.hasNext()) {
			KPFSFileInfo info = iter.next();
			if (info._sid == sid) {
				ips.remove(info);
			}
		}
	}
}
