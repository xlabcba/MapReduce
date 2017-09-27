package combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperWithCombiner extends Mapper<LongWritable, Text, Text, StationRecordWritableWithCombiner> {
	
	private static final String CSV_SPLITOR = ",";
	private static final String NUM_REGEX = "((-|\\+)?[0-9]+(\\.[0-9]+)?)+";
	private static final String TMAX = "TMAX";
	private static final String TMIN = "TMIN";
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] entry = value.toString().split(CSV_SPLITOR);
		if (isValidRecord(entry)) {
			Text stationId = new Text(entry[0]);
			String type = entry[2];
			Double reading = Double.parseDouble(entry[3]);
			StationRecordWritableWithCombiner record = new StationRecordWritableWithCombiner();
			if (type.equalsIgnoreCase(TMAX)) {
				record.addMaxRecord(reading, 1);
			} else if (type.equalsIgnoreCase(TMIN)) {
				record.addMinRecord(reading, 1);
			}
			context.write(stationId, record);
		}
	}
	
	/**
	 * Validate a record entry
	 * Typical entry is: StationId, Date, Type, Reading,...
	 * @param line
	 * @return
	 */
	public boolean isValidRecord(String[] entry) {
		// Check if has at least 4 cols
		if (entry.length < 4) {
			return false;
		}
		// Check if type is TMAX
		if (!entry[2].equalsIgnoreCase(TMAX) && !entry[2].equalsIgnoreCase(TMIN)) {
			return false;
		}
		// Check if reading is empty
		if (entry[3] == null || entry[3].isEmpty()) {
			return false;
		}
		// Check if reading is valid number
		if (!entry[3].matches(NUM_REGEX)) {
			return false;
		}
		return true;
	}

}
