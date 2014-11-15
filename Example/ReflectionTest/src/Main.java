import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;


public class Main {
	public static void main(String[] args) throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		File file  = new File("C:/Users/bbfee/Desktop/Android_Dev/meadow/git/MapReduce/MapReduce/jar/WordCounter.jar");
		URL url = file.toURL();  
		URL[] urls = new URL[]{url};
		ClassLoader cl = new URLClassLoader(urls);	
		Class cls = cl.loadClass("WordCounter");
//		Class<?>[] mapMethodClassArgs = {String[].class};
//		Method mapMethod = cls.getMethod("main");
		
		Constructor mapConstr = cls.getConstructor();
		Object mapper = mapConstr.newInstance();
		
//		mapMethod.invoke(mapper);
	}
}
