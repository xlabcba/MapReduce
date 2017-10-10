package noCombiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author lixie
 * Reducer class for nocombiner mode
 */
public class ReducerWithNoCombiner extends Reducer<Text, StationRecordWritableWithNoCombiner, Text, Text> {
	
	// Constants
	private static final String TMAX = "TMAX";
	private static final String TMIN = "TMIN";

	@Override
	public void reduce(Text key, Iterable<StationRecordWritableWithNoCombiner> values, Context context)
			throws IOException, InterruptedException {
		
		// Initialize accumu values
		double maxSum = new Double(0);
		double minSum = new Double(0);
		int maxCount = new Integer(0);
		int minCount = new Integer(0);

		// For each record r in input list, accumu value based on type
		for (StationRecordWritableWithNoCombiner record : values) {
			if (record.getType().toString().equalsIgnoreCase(TMAX)) {
				maxSum += record.getTemp();
				maxCount += 1;
			}
			if (record.getType().toString().equalsIgnoreCase(TMIN)) {
				minSum += record.getTemp();
				minCount += 1;
			}
		}
	
		// Calculate Averages from accumu values and create value string
		String maxAvg = maxCount != 0 ? String.valueOf(maxSum / maxCount) : "No max data";
		String minAvg = minCount != 0 ? String.valueOf(minSum / minCount) : "No min data";
		String result = maxAvg + ", " + minAvg;
		
		// emit (k3, v3)
		context.write(key, new Text(result));
	}
}
