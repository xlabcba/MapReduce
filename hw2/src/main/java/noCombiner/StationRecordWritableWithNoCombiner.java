package noCombiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StationRecordWritableWithNoCombiner implements Writable {
	
	private Text type;
	private double temp;
	
	public StationRecordWritableWithNoCombiner() {
		this.type = new Text();
		this.temp = new Double(0);
	}
	
	public StationRecordWritableWithNoCombiner(Text type, double temp) {
		this.type = type;
		this.temp = temp;
	}
	
	/**
	 * Set given temperature as a TMAX
	 * @param temp
	 */
	public void setMaxTemp(double temp) {
		this.type = new Text("TMAX");
		this.temp = temp;
	}
	
	/**
	 * Set given temperature as a TMIN
	 * @param temp
	 */
	public void setMinTemp(double temp) {
		this.type = new Text("TMIN");
		this.temp = temp;
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
		type.readFields(in);
		this.temp = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		type.write(out);
		out.writeDouble(temp);
	}

	@Override
	public String toString() {
		return "StationRecordWritable [type=" + type + ", temp=" + temp + "]";
	}

}
