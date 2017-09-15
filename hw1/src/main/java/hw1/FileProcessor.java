package hw1;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class FileProcessor {
	
	private static final String INPUT_FILE_PATH = "/Users/lixie/Documents/MapReduce/hw1/inputs/";
	private static final String OUTPUT_FILE_PATH = "/Users/lixie/Documents/MapReduce/hw1/outputs/";

	/**
	 * Unzip .csv.gz and read line by line
	 */
	public List<String> readFile(String gzFileName) {

		List<String> lines = new ArrayList<String>();
		BufferedReader br = null;
		String line = "";
		
		try {

			System.out.println("[Debug] Start Reading...");

			GZIPInputStream gzInput = new GZIPInputStream(new FileInputStream(INPUT_FILE_PATH + gzFileName));
			br = new BufferedReader(new InputStreamReader(gzInput));
			while ((line = br.readLine()) != null) {
				lines.add(line);
			}

			gzInput.close();
			System.out.println("[Debug] Done Reading!");

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return lines;
	}
	
}
