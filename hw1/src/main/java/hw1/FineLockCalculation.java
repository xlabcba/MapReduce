package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FineLockCalculation extends AbstractCalculation {
	
	private Map<String, StationRecord> stations;
	private Map<String, Double> averages;
	private long runtime;

	public FineLockCalculation() {
		// Using ConcurrentHashMap to have segment locking 
		// for better synchronization and performance on r/w
		this.stations = new ConcurrentHashMap<String, StationRecord>();
		this.averages = new HashMap<String, Double>();
		this.runtime = 0;	
	}

	@Override
	public void calculate(List<String> lines) {
		
		System.out.println("[Debug] Calculating in finelock...");

		// Generate indices for multi-thread tasks separation 
		int[][] indice= Utils.getPartitionIndices(lines.size());
		
		long startTime = System.currentTimeMillis();
		
		// Initialize workers and assign tasks
		List<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < Constants.THREAD_NUM; i++) {
			threads.add(new Thread(new Worker(i, lines.subList(indice[i][0], indice[i][1]))));
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
		// Calculate averages
		calculateAverages();
		
		runtime = System.currentTimeMillis() - startTime;
		
		if (Constants.PRINT_AVERAGES) {
			printAverages();
		}
	}
	
	/**
	 * Worker thread with fine lock
	 * receives records and accumulates to shared data structure 
	 */
	class Worker implements Runnable {

		// private int threadId;
		private List<String> lines;

		public Worker(int threadId, List<String> lines) {
			// this.threadId = threadId;
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
				// Read and initialize record in shared structure with 
				// segment locking on put and get operation 
			    StationRecord station = stations.get(stationId);
			    if (station == null) {
			        stations.putIfAbsent(stationId, new StationRecord(stationId));
			        station = stations.get(stationId);
			    }
				// Update record in shared structure
			    // with locking up the single entry station record
				synchronized (station) {
					station.addRecord(temperature);
				}
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
