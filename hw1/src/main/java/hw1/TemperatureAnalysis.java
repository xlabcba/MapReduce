package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TemperatureAnalysis {

	public static void main(String[] args) throws CloneNotSupportedException {

		// Parse Arguments
		String mode = "";
		try {
			mode = args[1];
		} catch (Exception e) {
			System.err.println("Please run with format: java -jar $$JAR_PATH -mode $$MODE -filename $$FILENAME");
			System.exit(1);
		}
		String filename = "";
		try {
			filename = args[3];
		} catch (Exception e) {
			System.err.println("Please run with format: java -jar $$JAR_PATH -mode $$MODE -filename $$FILENAME");
			System.exit(1);
		}

		// Read lines from input file
		List<String> lines = FileProcessor.readFile(filename);

		// Run Calculation in Corresponding Mode
		if (mode.equalsIgnoreCase("sequential")) {
			System.out.println("[Debug] Calculating Sequential...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				SequentialCalculation sc = new SequentialCalculation();
				sc.calculate(lines);
				long runtime = sc.getRuntime();
				minTime = Math.min(minTime, runtime);
				maxTime = Math.max(maxTime, runtime);
				sumTime += runtime;
			}
			System.out.println("[Debug] Sequential Mode" + " [averageTime=" + sumTime / Constants.MODE_RUN_TIME
					+ "ms, minTime=" + minTime + "ms, maxTime=" + maxTime + "ms]");
		}
		if (mode.equalsIgnoreCase("nolock")) {
			System.out.println("[Debug] Calculating No Lock...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				NoLockCalculation nlc = new NoLockCalculation();
				nlc.calculate(lines);
				long runtime = nlc.getRuntime();
				minTime = Math.min(minTime, runtime);
				maxTime = Math.max(maxTime, runtime);
				sumTime += runtime;
			}
			System.out.println("[Debug] No Lock Mode" + " [averageTime=" + sumTime / Constants.MODE_RUN_TIME
					+ "ms, minTime=" + minTime + "ms, maxTime=" + maxTime + "ms]");
		}
		if (mode.equalsIgnoreCase("coarselock")) {
			System.out.println("[Debug] Calculating Coarse Lock...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				CoarseLockCalculation clc = new CoarseLockCalculation();
				clc.calculate(lines);
				long runtime = clc.getRuntime();
				minTime = Math.min(minTime, runtime);
				maxTime = Math.max(maxTime, runtime);
				sumTime += runtime;
			}
			System.out.println("[Debug] Coarse Lock Mode" + " [averageTime=" + sumTime / Constants.MODE_RUN_TIME
					+ "ms, minTime=" + minTime + "ms, maxTime=" + maxTime + "ms]");
		}
		if (mode.equalsIgnoreCase("finelock")) {
			System.out.println("[Debug] Calculating Fine Lock...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				FineLockCalculation flc = new FineLockCalculation();
				flc.calculate(lines);
				long runtime = flc.getRuntime();
				minTime = Math.min(minTime, runtime);
				maxTime = Math.max(maxTime, runtime);
				sumTime += runtime;
			}
			System.out.println("[Debug] Fine Lock Mode" + " [averageTime=" + sumTime / Constants.MODE_RUN_TIME
					+ "ms, minTime=" + minTime + "ms, maxTime=" + maxTime + "ms]");
		}
		if (mode.equalsIgnoreCase("nosharing")) {
			System.out.println("[Debug] Calculating No Sharing...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				NoSharingCalculation nsc = new NoSharingCalculation();
				nsc.calculate(lines);
				long runtime = nsc.getRuntime();
				minTime = Math.min(minTime, runtime);
				maxTime = Math.max(maxTime, runtime);
				sumTime += runtime;
			}
			System.out.println("[Debug] No Sharing Mode" + " [averageTime=" + sumTime / Constants.MODE_RUN_TIME
					+ "ms, minTime=" + minTime + "ms, maxTime=" + maxTime + "ms]");
		}

		// [TODO] Write into file

		// System.out.println("[Debug] Main finished!");
	}

}
