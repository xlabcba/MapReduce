package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NoSharingCalculation implements Runnable {

	private int threadId;
	private Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
	private List<String> lines = new ArrayList<String>();

	public NoSharingCalculation(int threadId, Map<String, StationRecord> stations, List<String> lines) {
		this.threadId = threadId;
		this.stations = stations;
		this.lines = lines;
	}

	public void run() {

		String separator = ",";
		int counter = 1;

		for (String line : lines) {

			// Parse line
			String[] entry = line.split(separator);
			if (!Utils.isValidRecord(line)) continue;

			// Update record
			// System.out.println("[Debug] Updating station records...");
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
