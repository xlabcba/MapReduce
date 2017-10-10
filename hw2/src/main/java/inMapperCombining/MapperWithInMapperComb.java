package inMapperCombining;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
// import java.util.logging.Level;
// import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author lixie
 * Mapper class for inmappercomb mode
 */
public class MapperWithInMapperComb extends Mapper<LongWritable, Text, Text, StationRecordWritableWithInMapperComb> {
	
	// Constants
	private static final String CSV_SPLITOR = ",";
	private static final String NUM_REGEX = "((-|\\+)?[0-9]+(\\.[0-9]+)?)+";
	private static final String TMAX = "TMAX";
	private static final String TMIN = "TMIN";
	
	// private Logger logger;
	// Declaration of accumu hashmap, mapping from stationId to stationRecord
	private Map<Text, StationRecordWritableWithInMapperComb> map;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		// this.logger = Logger.getLogger(MapperWithInMapperComb.class.getName());
		// Initialization of accumu hashmap, mapping from stationId to stationRecord
		this.map = new HashMap<Text, StationRecordWritableWithInMapperComb>();
    }
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Parse line into entry and validate entry
		String[] entry = value.toString().split(CSV_SPLITOR);
		if (isValidRecord(entry)) {
			Text stationId = new Text(entry[0]);
			// Put a new entry (stationId, stationRecord) if it is not in hashmap
			if (!map.containsKey(stationId)) {
				map.put(stationId, new StationRecordWritableWithInMapperComb());
			}
			String type = entry[2];
			Double reading = Double.parseDouble(entry[3]);
			// Accumu record to value in corresp. entry in hashmap based on type
			if (type.equalsIgnoreCase(TMAX)) {
				map.get(stationId).addMaxRecord(reading, 1);
			} else if (type.equalsIgnoreCase(TMIN)) {
				map.get(stationId).addMinRecord(reading, 1);
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		
		// logger.log(Level.INFO, "Running cleanup...");
		// For each entry in accumu hashmap, emit (k1, v1)
		Iterator<Map.Entry<Text, StationRecordWritableWithInMapperComb>> itr = map.entrySet().iterator();	
		while (itr.hasNext()) {
			Entry<Text, StationRecordWritableWithInMapperComb> entry = itr.next();
			// logger.log(Level.INFO, "Entry: " + entry.toString());
			context.write(entry.getKey(), entry.getValue());
		}
		// logger.log(Level.INFO, "Finished cleanup!");
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
