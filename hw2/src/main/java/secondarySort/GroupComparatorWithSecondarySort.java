package secondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparatorWithSecondarySort extends WritableComparator {

	protected GroupComparatorWithSecondarySort() {
		super(KeyWritableWithSecondarySort.class, true);
	}
	
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    	KeyWritableWithSecondarySort k1 = (KeyWritableWithSecondarySort) w1;
    	KeyWritableWithSecondarySort k2 = (KeyWritableWithSecondarySort) w2;
    	return k1.getStationId().compareTo(k2.getStationId());
    }

}
