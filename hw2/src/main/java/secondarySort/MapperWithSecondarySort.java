package secondarySort;

import java.io.IOException;
// import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author lixie
 * Mapper class for secondary sort
 */
public class MapperWithSecondarySort extends Mapper<LongWritable, Text, CompositeKeyWithSecondarySort, StationRecordWritableWithSecondarySort> {

	// Constants
	private static final String CSV_SPLITOR = ",";
	private static final String NUM_REGEX = "((-|\\+)?[0-9]+(\\.[0-9]+)?)+";
	private static final String TMAX = "TMAX";
	private static final String TMIN = "TMIN";

	// private Logger logger = Logger.getLogger(MapperWithSecondarySort.class.getName());

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Parse line into entry and validate entry
		String[] entry = value.toString().split(CSV_SPLITOR);
		if (isValidRecord(entry)) {
			// Parse entry into fields
			Text stationId = new Text(entry[0]);
			int year = parseYear(entry[1]);
			Text type = new Text(entry[2]);
			Double reading = Double.parseDouble(entry[3]);
			// Create custom composite key with format (Text stationId, Int year)
			CompositeKeyWithSecondarySort stationYearPair = new CompositeKeyWithSecondarySort(stationId, year);
			// Create custom value with format (Int year, Text type, Double reading)
			StationRecordWritableWithSecondarySort record = new StationRecordWritableWithSecondarySort(year, type, reading);
			// emit (ck1, v1)
			context.write(stationYearPair, record);
		}
	}

	/**
	 * Validate a record entry Typical entry is: StationId, Date, Type,
	 * Reading,...
	 * 
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
	
	public int parseYear(String dateStr) {
//		Date date = DATE_FORMATTER.parseDateTime(dateStr).toDate();
//		Calendar cal = Calendar.getInstance();
//		cal.setTime(date);
//		return cal.get(Calendar.YEAR);
		return Integer.valueOf(dateStr.trim().substring(0, 4));
	}
}
