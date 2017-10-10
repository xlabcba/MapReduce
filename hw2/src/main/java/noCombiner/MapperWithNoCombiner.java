package noCombiner;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author lixie
 * Mapper class for nocombiner mode
 */
public class MapperWithNoCombiner extends Mapper<LongWritable, Text, Text, StationRecordWritableWithNoCombiner> {
	
	// Constants
	private static final String CSV_SPLITOR = ",";
	private static final String NUM_REGEX = "((-|\\+)?[0-9]+(\\.[0-9]+)?)+";
	private static final String TMAX = "TMAX";
	private static final String TMIN = "TMIN";
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Parse Line into entry and validate the entry
		String[] entry = value.toString().split(CSV_SPLITOR);
		if (isValidRecord(entry)) {
			// Parse valid entry into fields
			Text stationId = new Text(entry[0]);
			Text type = new Text(entry[2]);
			Double reading = Double.parseDouble(entry[3]);
			// Create custom value with format (String type, Double reading)
			StationRecordWritableWithNoCombiner record = new StationRecordWritableWithNoCombiner(type, reading);
			// emit (k1, v1)
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
