package secondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class KeyWritableWithSecondarySort implements WritableComparable<KeyWritableWithSecondarySort> {
	
	private Text stationId;
	private int year;

	public KeyWritableWithSecondarySort() {
		this.stationId = new Text();
		this.year = new Integer(0);
	}

	public KeyWritableWithSecondarySort(Text stationId, int year) {
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
		if (o instanceof KeyWritableWithSecondarySort) {
			KeyWritableWithSecondarySort other = (KeyWritableWithSecondarySort) o;
			return other.getStationId().equals(this.stationId) && other.getYear() == this.year;
		}
		return false;
	}
	
	@Override
	public int compareTo(KeyWritableWithSecondarySort other) {
			int cmp = this.stationId.compareTo(other.stationId);
			if (cmp != 0) {
				return cmp;
			}
			return compareInt(this.year, other.year);
		} 
		
		public int compareInt(int a, int b) {
			return  (a < b ? -1 : (a == b ? 0 : 1));
		}

	@Override
	public String toString() {
		return "KeyWritableWithSecondarySort [stationId=" + stationId + ", year=" + year + "]";
	}

}
