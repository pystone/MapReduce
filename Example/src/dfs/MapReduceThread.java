/**
 * 
 */
package dfs;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import mapreduce.MapReduceMasterThread;

/**
 * @author yinxu
 * 
 */
public class MapReduceThread extends Thread {

	private static boolean ongoing = true;
	
	@Override
	public void run() {
		ServerSocket ss;

		try {
			ss = new ServerSocket(YZFS.MP_PORT);
			while (ongoing) {
				Socket sock = ss.accept();
				MapReduceMasterThread masterThread = new MapReduceMasterThread(
						sock);
				masterThread.start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
