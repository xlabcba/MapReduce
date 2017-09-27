package inMapperCombining;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StationRecordWritableWithInMapperComb implements Writable {
	
	private double maxSum;
	private int maxCount;
	private double minSum;
	private int minCount;
	
	public StationRecordWritableWithInMapperComb() {
		this.maxSum = new Double(0);
		this.maxCount = new Integer(0);
		this.minSum = new Double(0);
		this.minCount = new Integer(0);
	}	
	
	public StationRecordWritableWithInMapperComb(double maxSum, int maxCount, double minSum, int minCount) {
		this.maxSum = maxSum;
		this.maxCount = maxCount;
		this.minSum = minSum;
		this.minCount = minCount;
	}
	
	public void addMaxRecord(double temp, int count) {
		this.maxSum += temp;
		this.maxCount += count;
	}
	
	public void addMaxRecord(StationRecordWritableWithInMapperComb other) {
		this.maxSum += other.maxSum;
		this.maxCount += other.maxCount;
	}
	
	public void addMinRecord(double temp, int count) {
		this.minSum += temp;
		this.minCount += count;
	}
	
	public void addMinRecord(StationRecordWritableWithInMapperComb other) {
		this.minSum += other.minSum;
		this.minCount += other.minCount;
	}
	
	public void addMaxAndMinRecord(StationRecordWritableWithInMapperComb other) {
		this.maxSum += other.maxSum;
		this.maxCount += other.maxCount;
		this.minSum += other.minSum;
		this.minCount += other.minCount;
	}
	
	public double getMaxSum() {
		return maxSum;
	}
	
	public void setMaxSum(double maxSum) {
		this.maxSum = maxSum;
	}
	
	public int getMaxCount() {
		return maxCount;
	}

	public void setMaxCount(int maxCount) {
		this.maxCount = maxCount;
	}

	public double getMinSum() {
		return minSum;
	}

	public void setMinSum(double minSum) {
		this.minSum = minSum;
	}

	public int getMinCount() {
		return minCount;
	}

	public void setMinCount(int minCount) {
		this.minCount = minCount;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.maxSum = in.readDouble();
		this.maxCount = in.readInt();
		this.minSum = in.readDouble();
		this.minCount = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(maxSum);
		out.writeInt(maxCount);
		out.writeDouble(minSum);
		out.writeInt(minCount);
	}

	@Override
	public String toString() {
		return "StationRecordWritable [maxSum=" + maxSum + ", maxCount=" + maxCount + ", minSum=" + minSum
				+ ", minCount=" + minCount + "]";
	}

}
