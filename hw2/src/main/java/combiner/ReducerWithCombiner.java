package combiner;

import java.io.IOException;
import java.util.logging.Level;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWithCombiner extends Reducer<Text, StationRecordWritableWithCombiner, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<StationRecordWritableWithCombiner> values, Context context)
			throws IOException, InterruptedException {

		StationRecordWritableWithCombiner combineRecord = new StationRecordWritableWithCombiner();
		for (StationRecordWritableWithCombiner record : values) {
			combineRecord.addMaxAndMinRecord(record);
		}
	
		String maxAvg = combineRecord.getMaxCount() != 0 ? String.valueOf(combineRecord.getMaxSum() / combineRecord.getMaxCount()) : "No max data";
		String minAvg = combineRecord.getMinCount() != 0 ? String.valueOf(combineRecord.getMinSum() / combineRecord.getMinCount()) : "No min data";
		String result = maxAvg + ", " + minAvg;
		context.write(key, new Text(result));
	}
}
