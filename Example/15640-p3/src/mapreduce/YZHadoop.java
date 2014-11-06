/**
 * 
 */
package mapreduce;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Queue;

import message.*;

import dfs.CommunicationModule;
import dfs.SlaveInfo;
import dfs.YZFS;

/**
 * @author yinxu main YZHadoop program running on top of YZFS file system
 */
public class YZHadoop {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if (args.length == 1) {
			/*
			 * args[0]: hadoop job name By default, take all files in YZFS as
			 * input files, and output to the root directory of YZFS as well.
			 * The default name is output
			 */
			NewJobMsg msg = new NewJobMsg(args[0]);
			try {

				AckMsg ack = null;
				ack = (AckMsg) CommunicationModule.sendMessage(getMasterIP(), YZFS.MP_PORT, msg);

				if (ack.isSuccessful()) {
					System.out.println("Sent out job: " + args[0]);
				} else {
					System.out.println("Sent out failed ");
				}

			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else {
			System.out.println("invalid arguments");
		}

	}

	private static InetAddress getMasterIP() throws FileNotFoundException, IOException {

		// load a properties file to read master ip and port
		Properties prop = new Properties();
		prop.load(new FileInputStream(YZFS.fileSystemWorkingDir + ".masterinfo.config"));
		return InetAddress.getByName(prop.getProperty("master host name"));
	}

}
