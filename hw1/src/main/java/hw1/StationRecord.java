package hw1;

public class StationRecord implements Cloneable {

	private String stationId;
	private double sum;
	private int count;
	private boolean isFibonacciOn = true;

	public StationRecord(String stationId, double temp, int count) {
		// [TODO] Will this count for adding to running sum by Fibonacci???
		this.stationId = stationId;
		this.sum = temp;
		this.count = count;
	}

	public Object clone() throws CloneNotSupportedException {
		return (StationRecord) super.clone();
	}

	public void addRecord(double temp, int count) {
		if (isFibonacciOn) {
			Utils.fibonacci(17);
		}
		this.sum += temp;
		this.count += count;
	}

	public synchronized void addRecordSafe(double temp, int count) {
		if (isFibonacciOn) {
			Utils.fibonacci(17);
		}
		this.sum += temp;
		this.count += count;
	}
	
	public void addRecord(StationRecord otherRecord) {
		this.sum += otherRecord.getSum();
		this.count += otherRecord.getCount();
	}

	public double calcAverage() {
		return this.sum / this.count;
	}

	public String getStationId() {
		return stationId;
	}

	public void setStationId(String stationId) {
		this.stationId = stationId;
	}

	public double getSum() {
		return sum;
	}

	public void setSum(double sum) {
		this.sum = sum;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public boolean isFibonacciOn() {
		return isFibonacciOn;
	}

	public void setFibonacciOn(boolean isFibonacciOn) {
		this.isFibonacciOn = isFibonacciOn;
	}

	@Override
	public String toString() {
		return "StationRecord [stationId=" + stationId + ", sum=" + sum + ", count=" + count + "]";
	}

}
