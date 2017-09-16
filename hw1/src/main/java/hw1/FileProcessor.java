package hw1;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class FileProcessor {

	private static final String INPUT_FILE_PATH = "/Users/lixie/Documents/MapReduce/hw1/inputs/";

	/**
	 * Read .csv.gz line by line
	 */
	public List<String> readFile(String gzFileName) {

		List<String> lines = new ArrayList<String>();
		BufferedReader br = null;
		String line = "";

		try {

			System.out.println("[Debug] Start Reading...");

			GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(INPUT_FILE_PATH + gzFileName));

			br = new BufferedReader(new InputStreamReader(gzis));
			while ((line = br.readLine()) != null) {
				lines.add(line);
			}

			gzis.close();
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

	/**
	 * Unzip .csv.gz & save as .csv file & read line by line
	 */
	public List<String> saveAndReadFile(String gzFileName) {

		List<String> lines = new ArrayList<String>();
		String line = "";
		String fileNameSplitor = "\\.";
		String fileNameJoiner = ".";

		String[] gzFileNameLst = gzFileName.split(fileNameSplitor);
		String csvFileName = String.join(fileNameJoiner,
				Arrays.copyOfRange(gzFileNameLst, 0, gzFileNameLst.length - 1));

		byte[] buffer = new byte[1024];

		try {

			System.out.println("[Debug] Start Unzip...");

			GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(INPUT_FILE_PATH + gzFileName));
			FileOutputStream out = new FileOutputStream(INPUT_FILE_PATH + csvFileName);

			int len;
			while ((len = gzis.read(buffer)) > 0) {
				out.write(buffer, 0, len);
			}

			gzis.close();
			out.close();

			System.out.println("[Debug] Done unzip!");

		} catch (IOException e) {
			e.printStackTrace();
		}

		try {

			System.out.println("[Debug] Start Reading csv...");

			BufferedReader br = new BufferedReader(new FileReader(INPUT_FILE_PATH + csvFileName));
			while ((line = br.readLine()) != null) {
				lines.add(line);
			}

			System.out.println("[Debug] Done Reading csv!");

		} catch (IOException e) {
			e.printStackTrace();
		}

		return lines;
	}

}
