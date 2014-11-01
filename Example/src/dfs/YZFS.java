package dfs;

import java.io.IOException;
import java.net.UnknownHostException;

import mapreduce.MapReduceMaster;

public class YZFS {

	/* to run, go to bin/ directory, type java dfs.YZFS */
	public static void main(String[] args) throws UnknownHostException,
			IOException, ClassNotFoundException, InterruptedException {
		/* start master server */
		if (args.length == 0) {
			MapReduceMaster mapReduceMaster = new MapReduceMaster();
			mapReduceMaster.start();
			
			MasterServer ms = new MasterServer();
			System.exit(0);
		}

		/* start background-running slave server */
		else if (args.length == 2 && args[0].equals("-c")) {
			SlaveServer ss = new SlaveServer(args[1]);
		}
		
		else if (args.length > 0 && args[0].equals("-yzfs")) {
			CommandLine commandLine = new CommandLine();
			commandLine.parseCommandLine(args);
		}

		else {
			System.out.println("Usage: java ProcessManager [-c <master hostname or ip>]");
		}
	}

	public static final int MASTER_PORT = 62762;
	public static final int SLAVE_PORT = 62763;
	public static final int CLIENT_PORT = 62764;
	
	public static final int replicationFactor = 2;
	
	public static final String fileSystemWorkingDir= "/tmp/YZFS/";
	
	public static final int RECORD_LENGTH = 12;
	public static final int NUM_RECORDS = 1000; /* # of records per chunk */
	
	public static final int MP_PORT = 62765;
	public static final int MP_DOWNLOAD_PORT = 62766;

}
