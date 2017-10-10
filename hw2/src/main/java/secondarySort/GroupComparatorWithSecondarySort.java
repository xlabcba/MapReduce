package secondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author lixie
 * GroupingComparator class for secondarysort mode
 * It defines which records are going to the same reduce call
 * The records with the same stationId in composite key are considered to be
 * in the same group of records to a reduce call
 */
public class GroupComparatorWithSecondarySort extends WritableComparator {

	protected GroupComparatorWithSecondarySort() {
		super(CompositeKeyWithSecondarySort.class, true);
	}
	
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    	CompositeKeyWithSecondarySort k1 = (CompositeKeyWithSecondarySort) w1;
    	CompositeKeyWithSecondarySort k2 = (CompositeKeyWithSecondarySort) w2;
    	// only stationId in composite is taken into consider
    	// which means records with same stationId are considered identical to a reduce call
    	return k1.getStationId().compareTo(k2.getStationId());
    }

}
