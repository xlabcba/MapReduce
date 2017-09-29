package secondarySort;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

public class MapperWithSecondarySort extends Mapper<LongWritable, Text, KeyWritableWithSecondarySort, StationRecordWritableWithSecondarySort> {

	private static final String CSV_SPLITOR = ",";
	private static final String NUM_REGEX = "((-|\\+)?[0-9]+(\\.[0-9]+)?)+";
	private static final String TMAX = "TMAX";
	private static final String TMIN = "TMIN";
	private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder().appendYear(4,4).appendMonthOfYear(2).appendDayOfMonth(2).toFormatter();

	private Logger logger = Logger.getLogger(MapperWithSecondarySort.class.getName());

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] entry = value.toString().split(CSV_SPLITOR);
		if (isValidRecord(entry)) {
			Text stationId = new Text(entry[0]);
			int year = parseYear(entry[1]);
			Text type = new Text(entry[2]);
			Double reading = Double.parseDouble(entry[3]);
			KeyWritableWithSecondarySort stationYearPair = new KeyWritableWithSecondarySort(stationId, year);
			StationRecordWritableWithSecondarySort record = new StationRecordWritableWithSecondarySort(year, type, reading);
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
