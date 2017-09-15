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
			System.out.println("[Debug] Thread" + threadId + " Line" + (counter++) + ":" + "[stationId=" + entry[0] + ", date=" + entry[1]
					+ ", type=" + entry[2] + ", value=" + entry[3]);
			if (!isValidRecord(line)) continue;

			// Update record
			// System.out.println("[Debug] Updating station records...");
			synchronized (stations) {
				if (!stations.containsKey(entry[0])) {
					stations.put(entry[0], new StationRecord(entry[0], Double.parseDouble(entry[3]), 1));
					continue;
				}
			}
			stations.get(entry[0]).addRecordSafe(Double.parseDouble(entry[3]), 1);
		}
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
