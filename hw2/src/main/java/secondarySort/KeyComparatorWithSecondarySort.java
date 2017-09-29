package secondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparatorWithSecondarySort extends WritableComparator {

	protected KeyComparatorWithSecondarySort() {
		super(KeyWritableWithSecondarySort.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		KeyWritableWithSecondarySort k1 = (KeyWritableWithSecondarySort) w1;
		KeyWritableWithSecondarySort k2 = (KeyWritableWithSecondarySort) w2;

		int cmp = k1.getStationId().compareTo(k2.getStationId());
		if (cmp != 0) {
			return cmp;
		}
		return (k1.getYear() < k2.getYear() ? -1 : (k1.getYear() == k2.getYear() ? 0 : 1));
	}

}
