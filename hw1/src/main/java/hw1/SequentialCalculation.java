package hw1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SequentialCalculation extends AbstractCalculation {
	
	private Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
	private Map<String, Double> averages = new HashMap<String, Double>();

	public void calculate(List<String> lines) {
		for (String line : lines) {
			// Validate record
			if (!Utils.isValidRecord(line)) continue;
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
		// Calculate averages
		calculateAverages();
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
