package noCombiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWithNoCombiner extends Reducer<Text, StationRecordWritableWithNoCombiner, Text, Text> {
	
	private static final String TMAX = "TMAX";
	private static final String TMIN = "TMIN";

	@Override
	public void reduce(Text key, Iterable<StationRecordWritableWithNoCombiner> values, Context context)
			throws IOException, InterruptedException {
		
		double maxSum = new Double(0);
		double minSum = new Double(0);
		int maxCount = new Integer(0);
		int minCount = new Integer(0);

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
	
		String maxAvg = maxCount != 0 ? String.valueOf(maxSum / maxCount) : "No max data";
		String minAvg = minCount != 0 ? String.valueOf(minSum / minCount) : "No min data";
		String result = maxAvg + ", " + minAvg;
		context.write(key, new Text(result));
	}
}
