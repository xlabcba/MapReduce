package secondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author lixie
 * Composite key class for secondary sort.
 * Its order and partition behavior is defined here
 * and overwritten in separate partitioner, keyComparatorand, and groupingComparator
 * which can be found in the same package
 */
public class CompositeKeyWithSecondarySort implements WritableComparable<CompositeKeyWithSecondarySort> {
	
	private Text stationId;
	private int year;

	public CompositeKeyWithSecondarySort() {
		this.stationId = new Text();
		this.year = new Integer(0);
	}

	public CompositeKeyWithSecondarySort(Text stationId, int year) {
		this.stationId = stationId;
		this.year = year;
	}

	public Text getStationId() {
		return stationId;
	}

	public void setStationId(Text stationId) {
		this.stationId = stationId;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		stationId.readFields(in);
		this.year = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		stationId.write(out);
		out.writeInt(this.year);	
	}
	
	@Override
	public boolean equals(Object o) {
		// equals is defined as both value of stationId and year are equal
		if (o instanceof CompositeKeyWithSecondarySort) {
			CompositeKeyWithSecondarySort other = (CompositeKeyWithSecondarySort) o;
			return other.getStationId().equals(this.stationId) && other.getYear() == this.year;
		}
		return false;
	}
	
	@Override
	public int compareTo(CompositeKeyWithSecondarySort other) {
		// order is defined as 
		// firstly sorting stationId in increasing order
		int cmp = this.stationId.compareTo(other.stationId);
		// then sorting year in increasing order if stationId are equal
		if (cmp == 0) {
				cmp = compareInt(this.year, other.year);
		}
		return cmp;
	} 
		
	public int compareInt(int a, int b) {
		return  (a < b ? -1 : (a == b ? 0 : 1));
	}

	@Override
	public String toString() {
		return "CompositeKeyWithSecondarySort [stationId=" + stationId + ", year=" + year + "]";
	}

}
