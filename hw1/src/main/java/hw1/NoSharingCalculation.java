package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hw1.NoLockCalculation.Worker;

public class NoSharingCalculation extends AbstractCalculation {
	
	private Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
	private Map<String, Double> averages = new HashMap<String, Double>();
	private long runtime = 0;

	public void calculate(List<String> lines) {

		int[][] indice= Utils.getPartitionIndice(lines.size());
		
		long startTime = System.currentTimeMillis();
		
		List<Thread> threads = new ArrayList<Thread>();
		List<HashMap<String, StationRecord>> threadStations = new ArrayList<HashMap<String, StationRecord>>();
		for (int i = 0; i < Constants.THREAD_NUM; i++) {
			threadStations.add(new HashMap<String, StationRecord>());
			threads.add(new Thread(new Worker(i, threadStations.get(i), lines.subList(indice[i][0], indice[i][1]))));		
		}

		for (int i = 0; i < Constants.THREAD_NUM; i++) {
			threads.get(i).start();
		}

		for (int i = 0; i < Constants.THREAD_NUM; i++) {
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
				String stationId = currStation.getStationId();
				if (!stations.containsKey(stationId)) {
					stations.put(stationId, new StationRecord(stationId));
				}
				stations.get(stationId).mergeRecords(currStation);
			}
		}
		
		// Calculate averages
		calculateAverages();
		
		runtime = System.currentTimeMillis() - startTime;
		
		if (Constants.PRINT_SUMMARY) {
			printSummary();
		}
	}

	class Worker implements Runnable {

		private int threadId;
		private List<String> lines = new ArrayList<String>();
		private Map<String, StationRecord> stations = new HashMap<String, StationRecord>();

		public Worker(int threadId, Map<String, StationRecord> stations, List<String> lines) {
			this.threadId = threadId;
			this.stations = stations;
			this.lines = lines;
		}

		public void run() {
			for (String line : lines) {
				// Validate record
				if (!Utils.isValidRecord(line))
					continue;
				// Parse line
				String[] entry = line.split(Constants.CSV_SEPARATOR);
				// Update record
				String stationId = entry[0];
				Double temperature = Double.parseDouble(entry[3]);
				if (!stations.containsKey(stationId)) {
					stations.put(stationId, new StationRecord(stationId));
				}
				stations.get(stationId).addRecord(temperature);
			}

		}
	}
	
	// [TODO] MODULARIZATION POSSIBLE???
	public void calculateAverages() {
		for (StationRecord station : this.stations.values()) {
			this.averages.put(station.getStationId(), station.calcAverage());
		}
	}
	
	public long getRuntime() {
		return this.runtime;
	}

	public void printSummary() {
		System.out.println("[Debug] ******************** SUMMARY ******************");
		for (StationRecord station : this.stations.values()) {
			System.out.println("[Debug] " + "[stationId=" + station.getStationId() + ", average="
					+ averages.get(station.getStationId()) + "]");
		}
	}

}
