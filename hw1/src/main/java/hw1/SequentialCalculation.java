package hw1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SequentialCalculation extends AbstractCalculation {
	
	private Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
	private Map<String, Double> averages = new HashMap<String, Double>();
	private long runtime = 0;

	public void calculate(List<String> lines) {
		
		long startTime = System.currentTimeMillis();
		
		for (String line : lines) {
			// Validate record
			if (!Utils.isValidRecord(line)) continue;
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
		// Calculate averages
		calculateAverages();
		
		runtime = System.currentTimeMillis() - startTime;
		
		if (Constants.PRINT_SUMMARY) {
			printSummary();
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
