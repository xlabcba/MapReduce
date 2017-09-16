package hw1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SequentialCalculation {
	
	private Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
	private Map<String, Double> averages = new HashMap<String, Double>();

	public Map<String, Double> calculate(List<String> lines) {

		String separator = ",";
		int counter = 1;

		for (String line : lines) {
			
			// Parse line
			String[] entry = line.split(separator);
			if (!Utils.isValidRecord(line)) continue;
			
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
		// System.out.println("[Debug] ******************** SUMMARY ******************");
		for (StationRecord station : stations.values()) {
			averages.put(station.getStationId(), station.calcAverage());
			// System.out.println("[Debug] " + "[stationId=" + station.getStationId() + ", average=" + averages.get(station.getStationId()) + "]");
		}

		return averages;
	}
}
