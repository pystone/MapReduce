///**
// * 
// */
//package hdfs;
//
//import java.io.BufferedInputStream;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.util.ArrayList;
//
//import mapreduce.GlobalInfo;
//
///**
// * @author PY
// *
// */
//public class KPFSStub {
//	private String _rootDir = "/Users/PY/Documents/cmu/f14/640/proj3/MapReduce/test_files/";
//	
//	public ArrayList<KPFile> getInputFiles(String taskName, Integer chunkSize) throws FileNotFoundException {
//		// TODO: rmi to get ArrayList<String>
//		ArrayList<String> paths = KPFileSplit.split(_rootDir + taskName + taskName, 
//				GlobalInfo.sharedInfo().FileChunkSizeB, _rootDir + taskName + "/InputFiles/", taskName);
//		
//		// local handle code
//		ArrayList<KPFile> ret = new ArrayList<KPFile>();
//		for (String path: paths) {
//			String curp = path.substring(path.lastIndexOf("/") + 1);
//			KPFile curf = new KPFile();
//			curf._relDir = taskName + "/InputFiles/";
//			curf._fileName = curp;
//			ret.add(curf);
//		}
//		return ret;
//	}
//	
//	public KPFile getFile(String relDir, String fileName) throws IOException {
//		// TODO: replace the following code to get the file content from fs server
//		String relPath = relDir + "/" + fileName;
//		String absPath = _rootDir + relPath;
//		File file = new File(absPath);
//		byte[] byteArr = new byte[(int)file.length()];
//		FileInputStream fin = new FileInputStream(file);
//		BufferedInputStream bin = new BufferedInputStream(fin);
//		bin.read(byteArr, 0, byteArr.length);
//		
//		// local handle code
//		File outFile = new File(GlobalInfo.sharedInfo().FileRootDir + relPath);
//		outFile.getParentFile().mkdirs();	/* create the parent directories */
//		FileOutputStream outStream = new FileOutputStream(outFile);
//		outStream.write(byteArr);
//		outStream.close();
//		
//		KPFile ret = new KPFile();
//		ret._fileName = fileName;
//		ret._relDir = relDir;
//		
//		return ret;
//	}
//	
//	public void saveFile(KPFile file) throws IOException {
//		// local handle code
//		String relPath = file.getRelPath();
//		String absPath = _rootDir + relPath;
//		byte[] byteArr = file.getByte();
//		
//		// TODO: replace the following code to save the file to fs server
//		File outFile = new File(_rootDir + file.getRelPath());
//		FileOutputStream outStream = new FileOutputStream(outFile);
//		outStream.write(byteArr);
//		outStream.close();
//	}
//}
