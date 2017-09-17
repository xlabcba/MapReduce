package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hw1.NoLockCalculation.Worker;

public class CoarseLockCalculation extends AbstractCalculation{
	
	private Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
	private Map<String, Double> averages = new HashMap<String, Double>();

	public void calculate(List<String> lines) {

		int[][] indice= Utils.getPartitionIndice(lines.size());
		
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
				synchronized (stations) {
					if (!stations.containsKey(entry[0])) {
						stations.put(entry[0], new StationRecord(stationId, temperature, 1));
					} else {
						stations.get(entry[0]).addRecord(temperature, 1);
					}
				}
			}
		}
	}
	
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
