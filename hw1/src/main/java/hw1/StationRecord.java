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

	/**
	 * Add one record for current station
	 * @param temperature
	 */
	public void addRecord(double temperature) {
		if (Constants.IS_FIBO_ON) {
			Utils.fibonacci(17);
		}
		this.sum += temperature;
		this.count += 1;
	}
	
	/**
	 * Merge a station record into current station record
	 * Both may have accumulated multiple records
	 * @param otherRecord
	 */
	public void mergeRecords(StationRecord otherRecord) {
		if (!this.stationId.equals(otherRecord.stationId)) {
			System.err.println("Two records from different station cannot be merged!");
			return;
		}
		this.sum += otherRecord.getSum();
		this.count += otherRecord.getCount();
	}

	/**
	 * Calculate averge temperature of current station
	 * @return
	 */
	public double calcAverage() {
		if (this.count == 0) {
			System.err.println("No data in this station!");
			System.exit(1);
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
