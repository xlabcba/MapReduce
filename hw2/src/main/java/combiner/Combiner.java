package combiner;

import java.io.IOException;
// import java.util.logging.Level;
// import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author lixie
 * Combiner class for combiner mode
 */
public class Combiner extends Reducer<Text, StationRecordWritableWithCombiner, Text, StationRecordWritableWithCombiner> {

	// private Logger logger = Logger.getLogger(Combiner.class.getName());

	@Override
	public void reduce(Text key, Iterable<StationRecordWritableWithCombiner> values, Context context)
			throws IOException, InterruptedException {

		// logger.log(Level.INFO, "Running in combiner...");
		// Initialize accumu record with format (Double maxSum, Int maxCnt, Double minSum, Int minCnt)
		StationRecordWritableWithCombiner combineRecord = new StationRecordWritableWithCombiner();
		
		// For each record r in input list, merge to accumu record with the same format
		for (StationRecordWritableWithCombiner record : values) {
			// logger.log(Level.INFO, "Combining record: " + record);
			combineRecord.addMaxAndMinRecord(record);
		}
		
		// logger.log(Level.INFO, "Finised in combiner!");
		// emit (k2, v2) without calculate averages
		context.write(key, combineRecord);
	}
}
