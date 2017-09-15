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
			System.out.println("[Debug] Line" + (counter++) + ":" + "[stationId=" + entry[0] + ", date=" + entry[1]
					+ ", type=" + entry[2] + ", value=" + entry[3]);
			if (!isValidRecord(line)) continue;
			
			// Update record
			System.out.println("[Debug] Updating station records...");
			if (!stations.containsKey(entry[0])) {
				stations.put(entry[0], new StationRecord(entry[0], Double.parseDouble(entry[3]), 1));
			} else {
				stations.get(entry[0]).addRecord(Double.parseDouble(entry[3]), 1);
			}		
		}
		
		// Calculate averages
		for (StationRecord station : stations.values()) {
			averages.put(station.getStationId(), station.calcAverage());
			System.out.println("[Debug] " + "[stationId=" + station.getStationId() + ", average=" + averages.get(station.getStationId()) + "]");
		}

		return averages;
	}
	
	public boolean isValidRecord(String line) {
		// [TODO] Double check understanding correctly or not!
		String separator = ",";
		String[] entry = line.split(separator);
		if (entry.length < 4) {
			return false;
		}
		if (!entry[2].equalsIgnoreCase("TMAX")) {
			return false;
		}
		if (!entry[3].matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)+")) {
			return false;
		}
		return true;
	}
}
