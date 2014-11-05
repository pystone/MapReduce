/**
 * 
 */
package hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * @author PY
 *
 */
public class HDFileSplit {
	public static ArrayList<String> split (String filePath, int chunkSizeB, String directory, String fileName) throws FileNotFoundException {
		File file = new File(filePath);
		Scanner scan = new Scanner(file);
		ArrayList<String> smallFiles = new ArrayList<String>();
		String curFile = "";
		String curLine = "";
		int partCnt = 0;
		
		while (scan.hasNextLine()) {
			for (; scan.hasNextLine() && curFile.length()<chunkSizeB; ) {
				curLine=scan.nextLine();
				curFile += curLine + '\n';
			}
			
			String curFileName = directory + fileName + ".part"+String.format("%03d", partCnt++);
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
			
			/* release the memory */
			curFile = curLine = "";
		}
		return smallFiles;
	}
}
