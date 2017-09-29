package secondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StationRecordWritableWithSecondarySort implements Writable {
	
	private int year;
	private Text type;
	private double temp;
	
	public StationRecordWritableWithSecondarySort() {
		this.year = new Integer(0);
		this.type = new Text();
		this.temp = new Double(0);
	}

	public StationRecordWritableWithSecondarySort(int year, Text type, double temp) {
		this.year = year;
		this.type = type;
		this.temp = temp;
	}
	
	public void setMaxTemp(int year, double temp) {
		this.year = year;
		this.type = new Text("TMAX");
		this.temp = temp;
	}
	
	public void setMinTemp(int year, double temp) {
		this.year = year;
		this.type = new Text("TMIN");
		this.temp = temp;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public Text getType() {
		return type;
	}

	public void setType(Text type) {
		this.type = type;
	}

	public double getTemp() {
		return temp;
	}

	public void setTemp(double temp) {
		this.temp = temp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		type.readFields(in);
		this.temp = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.year);
		type.write(out);
		out.writeDouble(this.temp);
	}

	@Override
	public String toString() {
		return "StationRecordWritableWithSecondarySort [year=" + year + ", type=" + type + ", temp=" + temp + "]";
	}

}
