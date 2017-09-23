package hw1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SequentialCalculation extends AbstractCalculation {
	
	private Map<String, StationRecord> stations;
	private Map<String, Double> averages;
	private long runtime;

	public SequentialCalculation() {
		this.stations = new HashMap<String, StationRecord>();
		this.averages = new HashMap<String, Double>();
		this.runtime = 0;	
	}

	@Override
	public void calculate(List<String> lines) {
		
		System.out.println("[Debug] Calculating in sequential...");
		
		long startTime = System.currentTimeMillis();
		
		for (String line : lines) {
			// Validate record
			if (!Utils.isValidRecord(line)) continue;
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
		// Calculate averages
		calculateAverages();
		
		runtime = System.currentTimeMillis() - startTime;
		
		if (Constants.PRINT_AVERAGES) {
			printAverages();
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
