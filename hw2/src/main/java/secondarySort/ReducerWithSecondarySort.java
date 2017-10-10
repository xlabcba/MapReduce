package secondarySort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author lixie
 * Reducer class for secondarysort mode
 * for each reduce call having the records with same stationId,
 * and based on the fact that records input list have year in increasing order,
 * one pass iteration can accumu records with same year and calculate corresp. averages
 */
public class ReducerWithSecondarySort extends Reducer<CompositeKeyWithSecondarySort, StationRecordWritableWithSecondarySort, Text, Text> {
	
	private static final String TMAX = "TMAX";
	private static final String TMIN = "TMIN";
	private static final String SEPARATOR = ", ";

	@Override
	public void reduce(CompositeKeyWithSecondarySort key, Iterable<StationRecordWritableWithSecondarySort> values, Context context)
			throws IOException, InterruptedException {
		
		// Initialize accumu list of string, each string with format "(year,minAvg,maxAvg)" 
		List<String> result = new ArrayList<String>();
		// Initialize accumu values
		int currYear = new Integer(0);
		double maxSum = new Double(0);
		int maxCount = new Integer(0);
		double minSum = new Double(0);
		int minCount = new Integer(0);

		// Iterator each record  r in the input list
		// The record has year in increasing order, which means same year record will come next to each other
		for (StationRecordWritableWithSecondarySort record : values) {
			// If current record has different year from last record
			// Another year group of records starts
			if (record.getYear() != currYear) {
				// If previous year accumu values are valid, and not the initial dummy ones
				// Calculate average and create output string to add to accumu list
				if (currYear != 0) {
					String maxAvg = maxCount != 0 ? String.valueOf(maxSum / maxCount) : "No max data";
					String minAvg = minCount != 0 ? String.valueOf(minSum / minCount) : "No min data";
					result.add("(" + currYear + SEPARATOR + minAvg + SEPARATOR + maxAvg + ")");
				}
				// Reset accumu values
				currYear = record.getYear(); 
				maxSum = new Double(0);
				maxCount = new Integer(0);
				minSum = new Double(0);
				minCount = new Integer(0);
			}	
			// Accumu current record values to accumu values based on type
			if (record.getType().toString().equalsIgnoreCase(TMAX)) {
				maxSum += record.getTemp();
				maxCount += 1;
			}
			if (record.getType().toString().equalsIgnoreCase(TMIN)) {
				minSum += record.getTemp();
				minCount += 1;
			}
		}
		// Cleanup rest of accumu values
		if (currYear != 0) {
			String maxAvg = maxCount != 0 ? String.valueOf(maxSum / maxCount) : "No max data";
			String minAvg = minCount != 0 ? String.valueOf(minSum / minCount) : "No min data";
			result.add("(" + currYear + SEPARATOR + minAvg + SEPARATOR + maxAvg + ")");
		}

		// emit (k3, v3)
		context.write(key.getStationId(), new Text("[" + String.join(SEPARATOR, result) + "]"));
	}
}	
