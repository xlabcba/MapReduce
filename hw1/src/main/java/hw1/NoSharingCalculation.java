package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hw1.NoLockCalculation.Worker;

public class NoSharingCalculation extends AbstractCalculation {
	
	private Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
	private Map<String, Double> averages = new HashMap<String, Double>();

	public void calculate(List<String> lines) {

		int[][] indice= Utils.getPartitionIndice(lines.size());
		
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
				if (!stations.containsKey(currStation.getStationId())) {
					// [TODO] Clone??? check cost compared with initialize new instance
					StationRecord newRecord;
					try {
						newRecord = (StationRecord) currStation.clone();
						stations.put(currStation.getStationId(), newRecord);
					} catch (CloneNotSupportedException e) {
						e.printStackTrace();
					}
				} else {
					stations.get(currStation.getStationId()).addRecord(currStation);
				}
			}
		}
		
		// Calculate averages
		calculateAverages();
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
				if (!stations.containsKey(entry[0])) {
					stations.put(entry[0], new StationRecord(stationId, temperature, 1));
				} else {
					stations.get(entry[0]).addRecord(temperature, 1);
				}
			}

		}
	}
	
	// [TODO] MODULARIZATION POSSIBLE???
	private void calculateAverages() {
		for (StationRecord station : this.stations.values()) {
			this.averages.put(station.getStationId(), station.calcAverage());
		}
	}

	public void printSummary() {
		System.out.println("[Debug] ******************** SUMMARY ******************");
		for (StationRecord station : this.stations.values()) {
			System.out.println("[Debug] " + "[stationId=" + station.getStationId() + ", average="
					+ averages.get(station.getStationId()) + "]");
		}
	}

}
