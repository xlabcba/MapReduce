package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TemperatureAnalysis {

	public static void main(String[] args) throws CloneNotSupportedException {

		// Read file from .csv.gz
		FileProcessor fr = new FileProcessor();
		// [TODO] file name as input
		List<String> lines = fr.readFile("1912.csv.gz");
	    
		// Calculate Average
		String mode = args[0];
		
		if (mode.equalsIgnoreCase("sequential") || mode.equalsIgnoreCase("all")) {
			System.out.println("[Debug] Calculating Sequential...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				long startTime = System.currentTimeMillis();
				SequentialCalculation sc = new SequentialCalculation();
				sc.calculate(lines);
				sc.printSummary();
				long costTime = System.currentTimeMillis() - startTime;
				minTime = Math.min(minTime, costTime);
				maxTime = Math.max(maxTime, costTime);
				sumTime += costTime;
			}
			System.out.println("[Debug] Sequential Mode" + "[averageTime=" + sumTime / Constants.MODE_RUN_TIME + "ms, minTime="
					+ minTime + "ms, maxTime=" + maxTime + "ms]");
		}
		if (mode.equalsIgnoreCase("nolock") || mode.equalsIgnoreCase("all")) {
			System.out.println("[Debug] Calculating No Lock...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				long startTime = System.currentTimeMillis();
				NoLockCalculation nlc = new NoLockCalculation();
				nlc.calculate(lines);
				nlc.printSummary();
				long costTime = System.currentTimeMillis() - startTime;
				minTime = Math.min(minTime, costTime);
				maxTime = Math.max(maxTime, costTime);
				sumTime += costTime;
			}
			System.out.println("[Debug] No Lock Mode" + "[averageTime=" + sumTime / Constants.MODE_RUN_TIME + "ms, minTime="
					+ minTime + "ms, maxTime=" + maxTime + "ms]");
		}
		if (mode.equalsIgnoreCase("coarselock") || mode.equalsIgnoreCase("all")) {
			System.out.println("[Debug] Calculating Coarse Lock...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				long startTime = System.currentTimeMillis();
				CoarseLockCalculation clc = new CoarseLockCalculation();
				clc.calculate(lines);
				clc.printSummary();
				long costTime = System.currentTimeMillis() - startTime;
				minTime = Math.min(minTime, costTime);
				maxTime = Math.max(maxTime, costTime);
				sumTime += costTime;
			}
			System.out.println("[Debug] Coarse Lock Mode" + "[averageTime=" + sumTime / Constants.MODE_RUN_TIME + "ms, minTime="
					+ minTime + "ms, maxTime=" + maxTime + "ms]");
		}
		if (mode.equalsIgnoreCase("finelock") || mode.equalsIgnoreCase("all")) {
			System.out.println("[Debug] Calculating Fine Lock...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				long startTime = System.currentTimeMillis();
				FineLockCalculation flc = new FineLockCalculation();
				flc.calculate(lines);
				flc.printSummary();
				long costTime = System.currentTimeMillis() - startTime;
				minTime = Math.min(minTime, costTime);
				maxTime = Math.max(maxTime, costTime);
				sumTime += costTime;
			}
			System.out.println("[Debug] Fine Lock Mode" + "[averageTime=" + sumTime / Constants.MODE_RUN_TIME + "ms, minTime=" + minTime
					+ "ms, maxTime=" + maxTime + "ms]");
		}
		if (mode.equalsIgnoreCase("nosharing") || mode.equalsIgnoreCase("all")) {
			System.out.println("[Debug] Calculating No Sharing...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			for (int i = 0; i < Constants.MODE_RUN_TIME; i++) {
				long startTime = System.currentTimeMillis();
				NoSharingCalculation nsc = new NoSharingCalculation();
				nsc.calculate(lines);
				nsc.printSummary();
				long costTime = System.currentTimeMillis() - startTime;
				minTime = Math.min(minTime, costTime);
				maxTime = Math.max(maxTime, costTime);
				sumTime += costTime;
			}
			System.out.println("[Debug] No Sharing Mode" + "[averageTime=" + sumTime / Constants.MODE_RUN_TIME + "ms, minTime="
					+ minTime + "ms, maxTime=" + maxTime + "ms]");
		}

		// [TODO] Write into file

		System.out.println("[Debug] Main finished!");
	}

}
