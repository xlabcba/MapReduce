package hw1;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class FileLoader {

	/**
	 * Read .csv.gz line by line
	 * @param gzFilePath
	 * @return
	 */
	public static List<String> loadFile(String gzFilePath) {

		List<String> lines = new ArrayList<String>();
		BufferedReader br = null;
		String line = "";

		// Load .csv.gz file into string arraylist
		try {

			// System.out.println("[Debug] Start Reading...");			

			GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(gzFilePath));

			br = new BufferedReader(new InputStreamReader(gzis));
			while ((line = br.readLine()) != null) {
				lines.add(line);
			}

			gzis.close();
			
			// System.out.println("[Debug] Done Reading!");

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
	 * A worked experiment
	 * Unzip .csv.gz & save as .csv file & read line by line
	 * @param gzFilePath
	 * @return
	 */
	public static List<String> loadAndSaveFile(String gzFilePath) {

		List<String> lines = new ArrayList<String>();
		String line = "";
		byte[] buffer = new byte[1024];
		BufferedReader br = null;

		// Generate csv file output path
		String[] gzFilePathLst = gzFilePath.split(Constants.FILENAME_SPLITOR);
		String csvFilePath = String.join(Constants.FILENAME_JOINER, Arrays.copyOfRange(gzFilePathLst, 0, gzFilePathLst.length - 1));

		// Unzip.csv.gz file and save as .csv file in the same folder
		try {

			// System.out.println("[Debug] Start Unzip...");		

			GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(gzFilePath));
			FileOutputStream out = new FileOutputStream(csvFilePath);

			int len;
			while ((len = gzis.read(buffer)) > 0) {
				out.write(buffer, 0, len);
			}

			gzis.close();
			out.close();

			// System.out.println("[Debug] Done unzip!");

		} catch (IOException e) {
			e.printStackTrace();
		}

		// Read .csv file and load as string arraylist
		try {

			// System.out.println("[Debug] Start Reading csv...");

			br = new BufferedReader(new FileReader(csvFilePath));
			while ((line = br.readLine()) != null) {
				lines.add(line);
			}

			// System.out.println("[Debug] Done Reading csv!");

		} catch (IOException e) {
			e.printStackTrace();
		}

		return lines;
	}

}
