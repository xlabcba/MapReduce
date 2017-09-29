package secondarySort;

import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner
		extends Partitioner<KeyWritableWithSecondarySort, StationRecordWritableWithSecondarySort> {

	@Override
	public int getPartition(KeyWritableWithSecondarySort key, StationRecordWritableWithSecondarySort value,
			int numPartitions) {
		return key.getStationId().hashCode() % numPartitions;
	}

}
