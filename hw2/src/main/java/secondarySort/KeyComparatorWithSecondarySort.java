package secondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author lixie
 * KeyComparator class for secondarysort mode
 * It defines the sorting order of the composite key
 * The records are firstly sorted in increasing order,
 * and records with same stationId are then sorted in increasing order
 */
public class KeyComparatorWithSecondarySort extends WritableComparator {

	protected KeyComparatorWithSecondarySort() {
		super(CompositeKeyWithSecondarySort.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWithSecondarySort k1 = (CompositeKeyWithSecondarySort) w1;
		CompositeKeyWithSecondarySort k2 = (CompositeKeyWithSecondarySort) w2;

		// stationId is firstly sorted in increasing order
		int cmp = k1.getStationId().compareTo(k2.getStationId());
		// year is then sorted in increasing order if stationId are equal
		if (cmp == 0) {
			cmp = (k1.getYear() < k2.getYear() ? -1 : (k1.getYear() == k2.getYear() ? 0 : 1));
		}
		return cmp;
	}

}
