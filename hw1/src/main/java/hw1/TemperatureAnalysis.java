package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TemperatureAnalysis {

	public static Map<String, Double> calcSequential(List<String> lines) {

		SequentialCalculation sc = new SequentialCalculation();
		return sc.calculate(lines);

	}

	public static Map<String, Double> calcNoLock(List<String> lines) {

		Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
		Map<String, Double> averages = new HashMap<String, Double>();

		// [TODO] check max thread #
		int taskNum = lines.size();
		int threadNum = 10;
		int tasksPerThread = taskNum / threadNum;
		int remainTasks = taskNum % threadNum;
		List<Thread> threads = new ArrayList<Thread>();

		// [TODO] check assignment of tasks
		int start = 0;
		int end = tasksPerThread;
		for (int i = 0; i < threadNum; i++) {
			end = start + tasksPerThread + (remainTasks-- > 0 ? 1 : 0);
			threads.add(new Thread(new NoLockCalculation(i, stations, lines.subList(start, end))));
			start = end;
		}

		for (int i = 0; i < threadNum; i++) {
			threads.get(i).start();
		}

		for (int i = 0; i < threadNum; i++) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// Calculate averages
		// System.out.println("[Debug] ******************** SUMMARY ******************");
		for (StationRecord station : stations.values()) {
			averages.put(station.getStationId(), station.calcAverage());
			// System.out.println("[Debug] " + "[stationId=" + station.getStationId() + ", average="
			// 		+ averages.get(station.getStationId()) + "]");
		}

		return averages;
	}

	public static Map<String, Double> calcCoarseLock(List<String> lines) {

		Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
		Map<String, Double> averages = new HashMap<String, Double>();

		// [TODO] check max thread #
		int taskNum = lines.size();
		int threadNum = 10;
		int tasksPerThread = taskNum / threadNum;
		int remainTasks = taskNum % threadNum;
		List<Thread> threads = new ArrayList<Thread>();

		// [TODO] check assignment of tasks
		int start = 0;
		int end = tasksPerThread;
		for (int i = 0; i < threadNum; i++) {
			end = start + tasksPerThread + (remainTasks-- > 0 ? 1 : 0);
			threads.add(new Thread(new CoarseLockCalculation(i, stations, lines.subList(start, end))));
			start = end;
		}

		for (int i = 0; i < threadNum; i++) {
			threads.get(i).start();
		}

		for (int i = 0; i < threadNum; i++) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// Calculate averages
		for (StationRecord station : stations.values()) {
			averages.put(station.getStationId(), station.calcAverage());
			// System.out.println("[Debug] " + "[stationId=" + station.getStationId() + ", average="
			// 		+ averages.get(station.getStationId()) + "]");
		}

		return averages;
	}

	public static Map<String, Double> calcFineLock(List<String> lines) {

		Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
		Map<String, Double> averages = new HashMap<String, Double>();

		// [TODO] check max thread #
		int taskNum = lines.size();
		int threadNum = 10;
		int tasksPerThread = taskNum / threadNum;
		int remainTasks = taskNum % threadNum;
		List<Thread> threads = new ArrayList<Thread>();

		// [TODO] check assignment of tasks
		int start = 0;
		int end = tasksPerThread;
		for (int i = 0; i < threadNum; i++) {
			end = start + tasksPerThread + (remainTasks-- > 0 ? 1 : 0);
			threads.add(new Thread(new FineLockCalculation(i, stations, lines.subList(start, end))));
			start = end;
		}

		for (int i = 0; i < threadNum; i++) {
			threads.get(i).start();
		}

		for (int i = 0; i < threadNum; i++) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// Calculate averages
		for (StationRecord station : stations.values()) {
			averages.put(station.getStationId(), station.calcAverage());
			// System.out.println("[Debug] " + "[stationId=" + station.getStationId() + ", average="
			// 		+ averages.get(station.getStationId()) + "]");
		}

		return averages;
	}

	public static Map<String, Double> calcNoSharing(List<String> lines) throws CloneNotSupportedException {

		Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
		Map<String, Double> averages = new HashMap<String, Double>();

		// [TODO] check max thread #
		int taskNum = lines.size();
		int threadNum = 10;
		int tasksPerThread = taskNum / threadNum;
		int remainTasks = taskNum % threadNum;
		List<Thread> threads = new ArrayList<Thread>();
		List<HashMap<String, StationRecord>> threadStations = new ArrayList<HashMap<String, StationRecord>>();

		// [TODO] check assignment of tasks
		int start = 0;
		int end = tasksPerThread;
		for (int i = 0; i < threadNum; i++) {
			end = start + tasksPerThread + (remainTasks-- > 0 ? 1 : 0);
			threadStations.add(new HashMap<String, StationRecord>());
			threads.add(new Thread(new NoSharingCalculation(i, threadStations.get(i), lines.subList(start, end))));
			start = end;
		}

		for (int i = 0; i < threadNum; i++) {
			threads.get(i).start();
		}

		for (int i = 0; i < threadNum; i++) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// Merge Results
		// System.out.println("[Debug] Merging reults...");
		int threadIndex = 0;
		for (Map<String, StationRecord> currThreadStations : threadStations) {
			// System.out.println("[Debug] Thread" + threadIndex++);
			for (StationRecord currStation : currThreadStations.values()) {
				// System.out.println("[Debug] " + currStation);
				if (!stations.containsKey(currStation.getStationId())) {
					stations.put(currStation.getStationId(), (StationRecord) currStation.clone());
				} else {
					stations.get(currStation.getStationId()).addRecord(currStation);
				}
			}
		}

		// Calculate averages
		for (StationRecord station : stations.values()) {
			averages.put(station.getStationId(), station.calcAverage());
			// System.out.println("[Debug] " + "[stationId=" + station.getStationId() + ", average="
			// 		+ averages.get(station.getStationId()) + "]");
		}

		return averages;
	}

	public static void main(String[] args) throws CloneNotSupportedException {

		// Read file from .csv.gz
		FileProcessor fr = new FileProcessor();
		List<String> lines = fr.readFile("1912.csv.gz");

	    
		// Calculate Average
		String mode = args[0];
		
		if (mode.equalsIgnoreCase("sequential") || mode.equalsIgnoreCase("all")) {
			System.out.println("[Debug] Calculating Sequential...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			int loopNum = 10;
			for (int i = 0; i < loopNum; i++) {
				long startTime = System.currentTimeMillis();
				Map<String, Double> averages = calcSequential(lines);
				long costTime = System.currentTimeMillis() - startTime;
				minTime = Math.min(minTime, costTime);
				maxTime = Math.max(maxTime, costTime);
				sumTime += costTime;
			}
			System.out.println("[Debug] Sequential Mode" + "[averageTime=" + sumTime / loopNum + "ms, minTime="
					+ minTime + "ms, maxTime=" + maxTime + "ms]");
		}
		if (mode.equalsIgnoreCase("nolock") || mode.equalsIgnoreCase("all")) {
			System.out.println("[Debug] Calculating No Lock...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			int loopNum = 10;
			for (int i = 0; i < loopNum; i++) {
				long startTime = System.currentTimeMillis();
				Map<String, Double> averages = calcNoLock(lines);
				long costTime = System.currentTimeMillis() - startTime;
				minTime = Math.min(minTime, costTime);
				maxTime = Math.max(maxTime, costTime);
				sumTime += costTime;
			}
			System.out.println("[Debug] No Lock Mode" + "[averageTime=" + sumTime / loopNum + "ms, minTime="
					+ minTime + "ms, maxTime=" + maxTime + "ms]");
		}
		if (mode.equalsIgnoreCase("coarselock") || mode.equalsIgnoreCase("all")) {
			System.out.println("[Debug] Calculating Coarse Lock...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			int loopNum = 10;
			for (int i = 0; i < loopNum; i++) {
				long startTime = System.currentTimeMillis();
				Map<String, Double> averages = calcCoarseLock(lines);
				long costTime = System.currentTimeMillis() - startTime;
				minTime = Math.min(minTime, costTime);
				maxTime = Math.max(maxTime, costTime);
				sumTime += costTime;
			}
			System.out.println("[Debug] Coarse Lock Mode" + "[averageTime=" + sumTime / loopNum + "ms, minTime="
					+ minTime + "ms, maxTime=" + maxTime + "ms]");
		}
		if (mode.equalsIgnoreCase("finelock") || mode.equalsIgnoreCase("all")) {
			System.out.println("[Debug] Calculating Fine Lock...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			int loopNum = 10;
			for (int i = 0; i < loopNum; i++) {
				long startTime = System.currentTimeMillis();
				Map<String, Double> averages = calcFineLock(lines);
				long costTime = System.currentTimeMillis() - startTime;
				minTime = Math.min(minTime, costTime);
				maxTime = Math.max(maxTime, costTime);
				sumTime += costTime;
			}
			System.out.println("[Debug] Fine Lock Mode" + "[averageTime=" + sumTime / loopNum + "ms, minTime=" + minTime
					+ "ms, maxTime=" + maxTime + "ms]");
		}
		if (mode.equalsIgnoreCase("nosharing") || mode.equalsIgnoreCase("all")) {
			System.out.println("[Debug] Calculating No Sharing...");
			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			double sumTime = 0;
			int loopNum = 10;
			for (int i = 0; i < loopNum; i++) {
				long startTime = System.currentTimeMillis();
				Map<String, Double> averages = calcNoSharing(lines);
				long costTime = System.currentTimeMillis() - startTime;
				minTime = Math.min(minTime, costTime);
				maxTime = Math.max(maxTime, costTime);
				sumTime += costTime;
			}
			System.out.println("[Debug] No Sharing Mode" + "[averageTime=" + sumTime / loopNum + "ms, minTime="
					+ minTime + "ms, maxTime=" + maxTime + "ms]");
		}

		// [TODO] Write into file

		System.out.println("[Debug] Main finished!");
	}

}
