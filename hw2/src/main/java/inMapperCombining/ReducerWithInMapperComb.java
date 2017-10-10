package inMapperCombining;

import java.io.IOException;
// import java.util.logging.Level;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author lixie
 * Reducer class for inmappercomb mode
 */
public class ReducerWithInMapperComb extends Reducer<Text, StationRecordWritableWithInMapperComb, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<StationRecordWritableWithInMapperComb> values, Context context)
			throws IOException, InterruptedException {

		// Initialize accumu record with format (Double maxSum, Int maxInt, Double minSum, Int minCnt)
		StationRecordWritableWithInMapperComb combineRecord = new StationRecordWritableWithInMapperComb();
		
		// For each record r in input list, merge into accumu record with same format
		for (StationRecordWritableWithInMapperComb record : values) {
			combineRecord.addMaxAndMinRecord(record);
		}
	
		// Calculate averages by accumu record and create output value string
		String maxAvg = combineRecord.getMaxCount() != 0 ? String.valueOf(combineRecord.getMaxSum() / combineRecord.getMaxCount()) : "No max data";
		String minAvg = combineRecord.getMinCount() != 0 ? String.valueOf(combineRecord.getMinSum() / combineRecord.getMinCount()) : "No min data";
		String result = maxAvg + ", " + minAvg;
		
		// emit (k3, v3)
		context.write(key, new Text(result));
	}
}
