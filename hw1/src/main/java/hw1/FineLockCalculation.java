package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FineLockCalculation implements Runnable {

	private int threadId;
	private Map<String, StationRecord> stations = new HashMap<String, StationRecord>();
	private List<String> lines = new ArrayList<String>();

	public FineLockCalculation(int threadId, Map<String, StationRecord> stations, List<String> lines) {
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
			String stationId = entry[0];
			Double temperature = Double.parseDouble(entry[3]);
			synchronized (stations) {
				if (!stations.containsKey(entry[0])) {
					stations.put(entry[0], new StationRecord(stationId, temperature, 1));
					continue;
				}
			}
			stations.get(entry[0]).addRecordSafe(temperature, 1);
		}
	}
}
