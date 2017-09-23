package hw1;

import java.util.List;
import java.util.Map;

public abstract class AbstractCalculation {

	/**
	 * Run sequential/multi-thread calculation including: 
	 * Accumulating station records and Calculating averages
	 * @param lines
	 */
	public abstract void calculate(List<String> lines);

	/**
	 * Calculate averages after accumulation 
	 * of each station record
	 */
	public abstract void calculateAverages();

	public abstract Map<String, Double> getAverages();

	public abstract long getRuntime();

	/**
	 * Print all (station, average) pairs
	 */
	public abstract void printAverages();

}
