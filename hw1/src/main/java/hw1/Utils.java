package hw1;

import java.util.Map;

public class Utils {
	
	/**
	 * Validate a record entry
	 * Typical entry is: StationId, Date, Type, Reading,...
	 * @param line
	 * @return
	 */
	public static boolean isValidRecord(String line) {
		String[] entry = line.split(Constants.CSV_SEPARATOR);
		// Check if has at least 4 cols
		if (entry.length < 4) {
			return false;
		}
		// Check if type is TMAX
		if (!entry[2].equalsIgnoreCase(Constants.VALID_TYPE)) {
			return false;
		}
		// Check if reading is empty
		if (entry[3] == null || entry[3].isEmpty()) {
			return false;
		}
		// Check if reading is valid number
		if (!entry[3].matches(Constants.NUM_REGEX)) {
			return false;
		}
		return true;
	}
	
	/**
	 * Generate records separation start and end indices
	 * based on Thread number
	 * @param inputSize
	 * @return
	 */
	public static int[][] getPartitionIndices(int inputSize) {
		// Get # of records per thread & Remaining records
		int tasksPerThread = inputSize / Constants.THREAD_NUM;
		int remainTasks = inputSize % Constants.THREAD_NUM;
		// Initiates like array of [startx, endx] for thread x 
		int[][] indice = new int[Constants.THREAD_NUM][2];
		// Loop to calculate start and end for each thread
		int start = 0, end;
		for (int i = 0; i < Constants.THREAD_NUM; i++) {
			// If there is still remaining records, assign 1 to current thread
			end = start + tasksPerThread + (remainTasks-- > 0 ? 1 : 0);
			indice[i][0] = start;
			indice[i][1] = end;
			start = end;
		}
		return indice;
	}
		
	/**
	 * Recursive Fibonacci calculation 
	 * to slowdown locking operations
	 * @param number
	 * @return
	 */
	public static int fibonacci(int number) {
		if (number == 1 || number == 2) {
			return 1;
		} 
		return fibonacci(number - 1) + fibonacci(number - 2);
	}
	
	/**
	 * Compare each station, average pair
	 * in multithread and sequential results
	 * @param sequential
	 * @param multithread
	 */
	public static void isCorrect(Map<String, Double> sequential, Map<String, Double> multithread) {
		System.out.println("[Debug] ******************** Verification ******************");
		boolean allCorrect = true;
		if (multithread == null) {
			allCorrect = false;
			System.out.println("[Debug] No Result Found");
			return;
		}
		if (sequential.entrySet().size() != multithread.entrySet().size()) {
			allCorrect = false;
			System.out.println("[Debug] Wrong Size: Expected[" + sequential.entrySet().size() +"], Actual[" + multithread.entrySet().size() + "]");
		}
		for (String key : sequential.keySet()) {
			if (!multithread.containsKey(key)) {
				allCorrect = false;
				System.out.println("[Debug] Key Not Found: Expected[" + key +"," + sequential.get(key) + "]");
				continue;
			}
			if (multithread.get(key) == null) {
				allCorrect = false;
				System.out.println("[Debug] Wrong Value: Expected[" + key +"," + sequential.get(key) + "] Actual[" + key + "," + multithread.get(key) + "]");
				continue;
			}
			if (Double.compare(multithread.get(key).doubleValue(), sequential.get(key).doubleValue()) != 0) {
				allCorrect = false;
				System.out.println("[Debug] Wrong Value: Expected[" + key +"," + sequential.get(key) + "] Actual[" + key + "," + multithread.get(key) + "]");
			}
		}
		if (allCorrect) {
			System.out.println("[Debug] Averages Correct!");
		}
	}
	
}
