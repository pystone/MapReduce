package server;

import hdfs.FileSystem;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Server implements FileSystem {
    
    public Server() {}

    public String readFile(String path) throws IOException {
    	FileInputStream fis = new FileInputStream(path);
    	BufferedReader br = new BufferedReader(new InputStreamReader(fis));
    	String line = null;
    	StringBuilder sb = new StringBuilder();
    	
    	while ((line = br.readLine()) != null) {
            sb.append(line);
            sb.append("\n");
        }
        return sb.toString();
    }
        
    public static void main(String args[]) {
        try {
            Server obj = new Server();
            FileSystem stub = (FileSystem) UnicastRemoteObject.exportObject(obj, 0);

            // Bind the remote object's stub in the registry
            Registry registry = null;
            try {
                registry = LocateRegistry.getRegistry(1099);//use any no. less than 55000
                registry.list();
                // This call will throw an exception if the registry does not already exist
            }
            catch (RemoteException e) { 
                registry = LocateRegistry.createRegistry(1099);
            }
            registry.bind("FileSystem", stub);

            System.out.println("Server ready");
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }
}