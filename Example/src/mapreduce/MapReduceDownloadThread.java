/**
 * 
 */
package mapreduce;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;

import dfs.YZFS;

/**
 * @author yinxu
 *
 */
public class MapReduceDownloadThread extends Thread{
	
	private MapReduceTask task;
	private ServerSocket dwldSocket;
	
	public MapReduceDownloadThread(MapReduceTask task, ServerSocket dwldSocket) {
		this.task = task;
		this.dwldSocket = dwldSocket;
	}
	
	@Override
	public void run() {
		try {
			
			// upload the intermediate file
			Socket uploadSocket = dwldSocket.accept();
			System.out.println("received download request from server");
			uploadResult(task, uploadSocket);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/* upload the intermediate result of mapper */
	public void uploadResult(MapReduceTask task, Socket sockFS) {
		
		//??? need to be changed, can't read all into memory
		FileInputStream fis;
		try {
			System.out.println("Upload filename is:" + task.getOutputFileName());
			fis = new FileInputStream(YZFS.fileSystemWorkingDir + task.getOutputFileName());
			
			OutputStream out = sockFS.getOutputStream();

			/* send byte array RECORD_LENGTH by RECORD_LENGTH, 
			 * starting from startIndex until endIndex */
			byte[] buffer = new byte[1024];
			int length = -1;

			while ( (length = fis.read(buffer) )> 0) {
				out.write(buffer, 0, length);
				out.flush();
			}
			sockFS.close();
			
			System.out.println("finish one upload");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
