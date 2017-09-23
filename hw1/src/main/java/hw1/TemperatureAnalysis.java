package hw1;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lixie
 * This class is an implementation of multi-version
 * temperature analysis. Given path to weather data records
 * and running mode, it will load data and calculating per-
 * station temperature average in the corresponding mode.
 * The mode can be sequential and multi-thread (nolock,
 * coarselock, finelock, and nosharing)
 */
public class TemperatureAnalysis {
	
	private static String mode;
	private static List<String> lines;
	private static List<String> output = new ArrayList<String>();
	private static long minTime = Long.MAX_VALUE;
	private static long maxTime = Long.MIN_VALUE;
	private static double sumTime = 0;
	
	/**
	 * Run tasks in given mode
	 * calculate runtimes and print summary
	 */
	public static void doCalculation() {
		
		System.out.println("[Debug] Running in " + mode + "...");

		// Run if debug turned on
		// calculate sequential avg for multithread avg comparation
		Map<String, Double> sequentialAvg = new HashMap<String, Double>();
		if (Constants.CHECK_RESULTS && !mode.equalsIgnoreCase("sequential")) {
			SequentialCalculation sc = new SequentialCalculation();
			sc.calculate(lines);
			sequentialAvg = sc.getAverages();
		}
		
		// Run calculation in given mode for certain times
		// and update Runtime factors
		AbstractCalculation calc;
		if (mode.equalsIgnoreCase("sequential")) {
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				calc = new SequentialCalculation();
				calc.calculate(lines);
				updateRuntime(calc.getRuntime());
				updateOutput(calc.getAverages());
			}
		} else if (mode.equalsIgnoreCase("nolock")) {
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				calc = new NoLockCalculation();
				calc.calculate(lines);
				// Run if debug turned on
				// Compare avg result with sequential result
				if (Constants.CHECK_RESULTS) {
					Utils.isCorrect(sequentialAvg, calc.getAverages());
				}
				updateRuntime(calc.getRuntime());
				updateOutput(calc.getAverages());
			}
		} else if (mode.equalsIgnoreCase("coarselock")) {
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				calc = new CoarseLockCalculation();
				calc.calculate(lines);
				// Run if debug turned on
				// Compare avg result with sequential result
				if (Constants.CHECK_RESULTS) {
					Utils.isCorrect(sequentialAvg, calc.getAverages());
				}
				updateRuntime(calc.getRuntime());
				updateOutput(calc.getAverages());
			}
		} else if (mode.equalsIgnoreCase("finelock")) {
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				calc = new FineLockCalculation();
				calc.calculate(lines);
				// Run if debug turned on
				// Compare avg result with sequential result
				if (Constants.CHECK_RESULTS) {
					Utils.isCorrect(sequentialAvg, calc.getAverages());
				}
				updateRuntime(calc.getRuntime());
				updateOutput(calc.getAverages());
			}
		} else if (mode.equalsIgnoreCase("nosharing")) {
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				calc = new NoSharingCalculation();
				calc.calculate(lines);
				// Run if debug turned on
				// Compare avg result with sequential result
				if (Constants.CHECK_RESULTS) {
					Utils.isCorrect(sequentialAvg, calc.getAverages());
				}
				updateRuntime(calc.getRuntime());
				updateOutput(calc.getAverages());
			}
		} else {
			System.err.println(
					"Invalid Mode: Please run define mode as one of following: [sequential, nolock, coarselock, finelock, nosharing]!");
			System.exit(1);
		}
		
		printRuntimeSummary();
	}

	/**
	 * Update runtime to Runtime factors
	 * @param runtime
	 */
	public static void updateRuntime(long runtime) {
		minTime = Math.min(minTime, runtime);
		maxTime = Math.max(maxTime, runtime);
		sumTime += runtime;
	}

	public static void updateOutput(Map<String, Double> averages) {
		output.add("Result of a run of calculation in " + mode + " mode:");
		for (String stationId : averages.keySet()) {
			String newLine = "[stationId=" + stationId + ", " + "average=" + averages.get(stationId) + "]";
			output.add(newLine);
		}
		output.add("\n");
	}
	
	public static void printRuntimeSummary() {
		System.out.println("[Debug] ******************** RunTime Summary ******************");
		String newLine = mode + " Mode " + "[averageTime=" + sumTime / Constants.MODE_RUN_TIME + "ms, minTime=" + minTime + "ms, maxTime=" + maxTime + "ms]";
		System.out.println("[Debug] " + newLine);
		output.add("Runtime Summary: ");
		output.add(newLine + "\n");	
	}
	
	public static void writeIntoFile(){
	    String PATH = new File("").getAbsolutePath();
	    String directoryName = PATH + "/output";
	    String fileName = mode + "_output_" + System.currentTimeMillis() + ".txt";

	    File directory = new File(directoryName);
	    if (!directory.exists()){
	        directory.mkdir();
	    }

	    File file = new File(directoryName + "/" + fileName);
	    try{
	        FileWriter fw = new FileWriter(file.getAbsoluteFile());
	        BufferedWriter bw = new BufferedWriter(fw);
	        for (String line : output) {
		        bw.write(line + "\n");
	        }
	        bw.close();
	    }
	    catch (IOException e){
	        e.printStackTrace();
	        System.exit(-1);
	    }
	}

	/**
	 * MainClass that parses args, load file,
	 * and assign tasks to calculation version 
	 * @param args
	 */
	public static void main(String[] args) {

		// Parse Arguments
		String inputPath = "";
		try {
			inputPath = args[1];
		} catch (Exception e) {
			System.err.println(
					"Invalid Input: Please run with format: java -jar ${jar.path} -input ${input.path} -mode sequential -fibo ${fibo.on} -printAvg ${debug.printAvg} -checkAvg ${debug.checkAvg}");
			e.printStackTrace();
			System.exit(1);
		}
		try {
			mode = args[3];
		} catch (Exception e) {
			System.err.println(
					"Invalid Input: Please run with format: java -jar ${jar.path} -input ${input.path} -mode sequential -fibo ${fibo.on} -printAvg ${debug.printAvg} -checkAvg ${debug.checkAvg}");
			e.printStackTrace();
			System.exit(1);
		}
		try {
			Constants.IS_FIBO_ON = Boolean.valueOf(args[5].toLowerCase());
		} catch (Exception e) {
			System.err.println(
					"Invalid Input: Please run with format: java -jar ${jar.path} -input ${input.path} -mode sequential -fibo ${fibo.on} -printAvg ${debug.printAvg} -checkAvg ${debug.checkAvg}");
			e.printStackTrace();
			System.exit(1);
		}
		try {
			Constants.PRINT_AVERAGES = Boolean.valueOf(args[7].toLowerCase());
		} catch (Exception e) {
			System.err.println(
					"Invalid Input: Please run with format: java -jar ${jar.path} -input ${input.path} -mode sequential -fibo ${fibo.on} -printAvg ${debug.printAvg} -checkAvg ${debug.checkAvg}");
			e.printStackTrace();
			System.exit(1);
		}
		try {
			Constants.CHECK_RESULTS = Boolean.valueOf(args[9].toLowerCase());
		} catch (Exception e) {
			System.err.println(
					"Invalid Input: Please run with format: java -jar ${jar.path} -input ${input.path} -mode sequential -fibo ${fibo.on} -printAvg ${debug.printAvg} -checkAvg ${debug.checkAvg}");
			e.printStackTrace();
			System.exit(1);
		}

		// Read lines from input file
		lines = FileLoader.loadFile(inputPath);

		// Run Calculation in Corresponding Mode
		doCalculation();
		
		writeIntoFile();

		// System.out.println("[Debug] Main finished!");
	}

}
