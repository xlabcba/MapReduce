package hw1;

public class StationRecord {

	private String stationId;
	private double sum;
	private int count;

	public StationRecord(String stationId) {
		this.stationId = stationId;
		this.sum = 0;
		this.count = 0;
	}

	public void addRecord(double temp) {
		if (Constants.IS_FIBO_ON) {
			Utils.fibonacci(17);
		}
		this.sum += temp;
		this.count += 1;
	}

	public synchronized void addRecordSafe(double temp) {
		if (Constants.IS_FIBO_ON) {
			Utils.fibonacci(17);
		}
		this.sum += temp;
		this.count += 1;
	}
	
	public void mergeRecords(StationRecord otherRecord) {
		if (!this.stationId.equals(otherRecord.stationId)) {
			throw new IllegalArgumentException("Two records from different station cannot be merged!");
		}
		this.sum += otherRecord.getSum();
		this.count += otherRecord.getCount();
	}

	public double calcAverage() {
		if (this.count == 0) {
			throw new IllegalArgumentException("No data in this station!");
		}
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

	@Override
	public String toString() {
		return "StationRecord [stationId=" + stationId + ", sum=" + sum + ", count=" + count + "]";
	}

}
