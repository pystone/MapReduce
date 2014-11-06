package example;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;


public class RandomCharacterGenerator {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Random random = new Random();
		
		/* 33 ~ 126 */
		File file = new File("bin/inputs/file0.txt");
		FileWriter fileWriter = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
		
		int i = 0;
		while (i++ < 9000) {
			bufferWriter.write((char) (random.nextInt(94) + 33));
			bufferWriter.write(' ');
			bufferWriter.write((char) (random.nextInt(94) + 33));
			bufferWriter.write(' ');
			bufferWriter.write((char) (random.nextInt(94) + 33));
			bufferWriter.write(' ');
			bufferWriter.write((char) (random.nextInt(94) + 33));
			bufferWriter.write(' ');
			bufferWriter.write((char) (random.nextInt(94) + 33));
			bufferWriter.write(' ');
			bufferWriter.write((char) (random.nextInt(94) + 33));
			bufferWriter.write('\n');
		}
		bufferWriter.close();
		
	}

}
