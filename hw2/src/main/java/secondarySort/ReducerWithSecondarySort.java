package secondarySort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import noCombiner.StationRecordWritableWithNoCombiner;

public class ReducerWithSecondarySort extends Reducer<KeyWritableWithSecondarySort, StationRecordWritableWithSecondarySort, Text, Text> {
	
	private static final String TMAX = "TMAX";
	private static final String TMIN = "TMIN";

	@Override
	public void reduce(KeyWritableWithSecondarySort key, Iterable<StationRecordWritableWithSecondarySort> values, Context context)
			throws IOException, InterruptedException {
		
		List<String> result = new ArrayList<String>();
		int currYear = new Integer(0);
		double maxSum = new Double(0);
		int maxCount = new Integer(0);
		double minSum = new Double(0);
		int minCount = new Integer(0);

		for (StationRecordWritableWithSecondarySort record : values) {
			if (record.getYear() != currYear) {
				if (currYear != 0) {
					String maxAvg = maxCount != 0 ? String.valueOf(maxSum / maxCount) : "No max data";
					String minAvg = minCount != 0 ? String.valueOf(minSum / minCount) : "No min data";
					result.add("(" + currYear + ", " + minAvg + ", " + maxAvg + ")");
				}
				currYear = record.getYear(); 
				maxSum = new Double(0);
				maxCount = new Integer(0);
				minSum = new Double(0);
				minCount = new Integer(0);
			}
			if (record.getType().toString().equalsIgnoreCase(TMAX)) {
				maxSum += record.getTemp();
				maxCount += 1;
			}
			if (record.getType().toString().equalsIgnoreCase(TMIN)) {
				minSum += record.getTemp();
				minCount += 1;
			}
		}
		if (currYear != 0) {
			String maxAvg = maxCount != 0 ? String.valueOf(maxSum / maxCount) : "No max data";
			String minAvg = minCount != 0 ? String.valueOf(minSum / minCount) : "No min data";
			result.add("(" + currYear + ", " + minAvg + ", " + maxAvg + ")");
		}

		context.write(key.getStationId(), new Text("[" + String.join(", ", result) + "]"));
	}
}	
