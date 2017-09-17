package hw1;

public class Utils {

	public static boolean isValidRecord(String line) {
		// [TODO] Double check understanding correctly or not!
		String separator = ",";
		String[] entry = line.split(separator);
		if (entry.length < 4) {
			return false;
		}
		if (!entry[2].equalsIgnoreCase("TMAX")) {
			return false;
		}
		if (entry[3] == null || entry[3].isEmpty()) {
			return false;
		}
		if (!entry[3].matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)+")) {
			return false;
		}
		return true;
	}
	
	public static int[][] getPartitionIndice(int inputSize) {
		// [TODO] check max thread #;
		int tasksPerThread = inputSize / Constants.THREAD_NUM;
		int remainTasks = inputSize % Constants.THREAD_NUM;
		// [TODO] check assignment of tasks
		int[][] indice = new int[Constants.THREAD_NUM][2];
		int start = 0, end;
		for (int i = 0; i < Constants.THREAD_NUM; i++) {
			end = start + tasksPerThread + (remainTasks-- > 0 ? 1 : 0);
			indice[i][0] = start;
			indice[i][1] = end;
			start = end;
		}
		return indice;
	}
		
	public static int fibonacci(int number) {
		if (number == 1 || number == 2) {
			return 1;
		} 
		return fibonacci(number - 1) + fibonacci(number - 2);
	}
	
}
