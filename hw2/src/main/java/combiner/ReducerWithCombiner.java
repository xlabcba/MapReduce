package combiner;

import java.io.IOException;
// import java.util.logging.Level;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author lixie
 *	Reducer class for combiner mode
 */
public class ReducerWithCombiner extends Reducer<Text, StationRecordWritableWithCombiner, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<StationRecordWritableWithCombiner> values, Context context)
			throws IOException, InterruptedException {

		// Initialize accumu record with format (Double maxSum, Int maxCnt, Double minSum, Int minCnt)
		StationRecordWritableWithCombiner combineRecord = new StationRecordWritableWithCombiner();
		
		// For each record r in input list, merge into accumu record with the same format
		for (StationRecordWritableWithCombiner record : values) {
			combineRecord.addMaxAndMinRecord(record);
		}
	
		// Calculate averages by using accumu record and create output value string
		String maxAvg = combineRecord.getMaxCount() != 0 ? String.valueOf(combineRecord.getMaxSum() / combineRecord.getMaxCount()) : "No max data";
		String minAvg = combineRecord.getMinCount() != 0 ? String.valueOf(combineRecord.getMinSum() / combineRecord.getMinCount()) : "No min data";
		String result = maxAvg + ", " + minAvg;
		
		// emit (k3, v3)
		context.write(key, new Text(result));
	}
}
