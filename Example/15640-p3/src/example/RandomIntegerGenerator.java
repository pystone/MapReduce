package example;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class RandomIntegerGenerator {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Random random = new Random();
		
		/* 33 ~ 126 */
		File file = new File("bin/inputs/file1.txt");
		FileWriter fileWriter = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
		
		int count1 = 0;
		int count2 = 0;
		int i;
		String str;
		while (count1++ < 9000) {
			i = random.nextInt();
			str = "" + i;
			for (count2 = str.length(); count2 < 12; count2++)
				str += ' ';
			bufferWriter.write(str);
			bufferWriter.write('\n');
		}
		bufferWriter.close();
	}

}
