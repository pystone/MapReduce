package mapreduce;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import dfs.YZFS;

public class MapReduceMaster extends Thread{
	
	public void run() {
		ServerSocket serverSocket;

		try {
			serverSocket = new ServerSocket(YZFS.MP_PORT);
			while (true) {
				Socket sock = serverSocket.accept();
				MapReduceMasterThread masterThread = new MapReduceMasterThread(sock);
				masterThread.start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
