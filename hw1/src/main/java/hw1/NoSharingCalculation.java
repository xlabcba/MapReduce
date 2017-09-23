package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NoSharingCalculation extends AbstractCalculation {
	
	private Map<String, StationRecord> stations;
	private Map<String, Double> averages;
	private long runtime;

	public NoSharingCalculation() {
		this.stations = new HashMap<String, StationRecord>();
		this.averages = new HashMap<String, Double>();
		this.runtime = 0;	
	}

	@Override
	public void calculate(List<String> lines) {
		
		System.out.println("[Debug] Calculating in nosharing...");

		// Generate indices for multi-thread tasks separation 
		int[][] indice= Utils.getPartitionIndices(lines.size());
		
		long startTime = System.currentTimeMillis();
		
		// Initialize workers, and their own accumulating data structure, and assign tasks
		List<Thread> threads = new ArrayList<Thread>();
		List<HashMap<String, StationRecord>> threadStations = new ArrayList<HashMap<String, StationRecord>>();
		for (int i = 0; i < Constants.THREAD_NUM; i++) {
			threadStations.add(new HashMap<String, StationRecord>());
			threads.add(new Thread(new Worker(i, threadStations.get(i), lines.subList(indice[i][0], indice[i][1]))));		
		}
		// Start run of workers
		for (int i = 0; i < Constants.THREAD_NUM; i++) {
			threads.get(i).start();
		}
		// Wait workers to terminate
		for (int i = 0; i < Constants.THREAD_NUM; i++) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}	
		// Merge Results
		merageAccumuResults(threadStations);	
		// Calculate averages
		calculateAverages();
		
		runtime = System.currentTimeMillis() - startTime;
		
		if (Constants.PRINT_AVERAGES) {
			printAverages();
		}
	}

	/**
	 * Worker thread without lock
	 * receives records and accumulates to its own data structure 
	 */
	class Worker implements Runnable {

		// private int threadId;
		private List<String> lines;
		// Each worker has its own accumulation data structure
		private Map<String, StationRecord> stations;

		public Worker(int threadId, Map<String, StationRecord> stations, List<String> lines) {
			// this.threadId = threadId;
			// Initialize its accumulation data structure
			this.stations = stations;
			this.lines = lines;
		}

		@Override
		public void run() {
			for (String line : lines) {
				// Validate record
				if (!Utils.isValidRecord(line)) continue;
				// Parse line
				String[] entry = line.split(Constants.CSV_SEPARATOR);
				String stationId = entry[0];
				Double temperature = Double.parseDouble(entry[3]);
				// Update record in its own data structure without lock
				if (!stations.containsKey(stationId)) {
					stations.put(stationId, new StationRecord(stationId));
				}
				stations.get(stationId).addRecord(temperature);
			}

		}
	}
	
	/**
	 * Merge station records accumulated by all thread
	 * into one set of station records
	 * @param threadStations
	 */
	public void merageAccumuResults(List<HashMap<String, StationRecord>> threadStations) {
		// Iterate set of records accumulated by each thread
		for (Map<String, StationRecord> currThreadStations : threadStations) {
			// Iterate record in the set from same thread
			for (StationRecord currStation : currThreadStations.values()) {
				String stationId = currStation.getStationId();
				// Merge the station record into final station record map
				if (!stations.containsKey(stationId)) {
					stations.put(stationId, new StationRecord(stationId));
				}
				stations.get(stationId).mergeRecords(currStation);
			}
		}
	}
	
	@Override
	public void calculateAverages() {
		for (StationRecord station : this.stations.values()) {
			if (station.getCount() != 0) {
				this.averages.put(station.getStationId(), station.calcAverage());
			}
		}
	}
	
	@Override
	public Map<String, Double> getAverages() {
		return this.averages;
	}
	
	@Override
	public long getRuntime() {
		return this.runtime;
	}

	@Override
	public void printAverages() {
		System.out.println("[Debug] ******************** Averages ******************");
		for (StationRecord station : this.stations.values()) {
			System.out.println("[Debug] " + "[stationId=" + station.getStationId() + ", average="
					+ averages.get(station.getStationId()) + "]");
		}
	}

}
