package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NoLockCalculation extends AbstractCalculation {
	
	private Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
	private Map<String, Double> averages = new HashMap<String, Double>();
	private long runtime = 0;

	public void calculate(List<String> lines) {

		int[][] indice= Utils.getPartitionIndice(lines.size());
		
		long startTime = System.currentTimeMillis();
		
		List<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < Constants.THREAD_NUM; i++) {
			threads.add(new Thread(new Worker(i, lines.subList(indice[i][0], indice[i][1]))));
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

		public Worker(int threadId, List<String> lines) {
			this.threadId = threadId;
			this.lines = lines;
		}

		public void run() {
			for (String line : lines) {
				// Validate record
				if (!Utils.isValidRecord(line))
					continue;
				// Parse line
				String[] entry = line.split(Constants.CSV_SEPARATOR);
				String stationId = entry[0];
				Double temperature = Double.parseDouble(entry[3]);
				// Update record
				if (!stations.containsKey(stationId)) {
					stations.put(stationId, new StationRecord(stationId));
				}
				stations.get(stationId).addRecord(temperature);
			}
		}
	}
	
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
