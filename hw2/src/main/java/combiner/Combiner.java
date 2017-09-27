package combiner;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner extends Reducer<Text, StationRecordWritableWithCombiner, Text, StationRecordWritableWithCombiner> {

	private Logger logger = Logger.getLogger(Combiner.class.getName());

	@Override
	public void reduce(Text key, Iterable<StationRecordWritableWithCombiner> values, Context context)
			throws IOException, InterruptedException {

		logger.log(Level.INFO, "Running in combiner...");
		StationRecordWritableWithCombiner combineRecord = new StationRecordWritableWithCombiner();
		for (StationRecordWritableWithCombiner record : values) {
			logger.log(Level.INFO, "Combining record: " + record);
			combineRecord.addMaxAndMinRecord(record);
		}
		logger.log(Level.INFO, "Finised in combiner!");
		context.write(key, combineRecord);
	}
}
