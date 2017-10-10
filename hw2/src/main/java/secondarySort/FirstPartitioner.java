package secondarySort;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author lixie
 * Partitioner class for secondarysort mode
 * It defines the partition behavior of the job,
 * which is only partitioned by stationId in the composite key,
 * which means the record with same stationId and different year 
 * are considered to be assigned to the same reducer
 */
public class FirstPartitioner
		extends Partitioner<CompositeKeyWithSecondarySort, StationRecordWritableWithSecondarySort> {

	@Override
	public int getPartition(CompositeKeyWithSecondarySort key, StationRecordWritableWithSecondarySort value,
			int numPartitions) {
		// Partition by only stationId in the composite key and ignore year
		// Integer.MAX_VALUE to avoid illegal partition number (< 0)
		return (key.getStationId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
