package hdfs;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface FileSystem extends Remote {
	String readFile(String path) throws RemoteException, IOException;
}