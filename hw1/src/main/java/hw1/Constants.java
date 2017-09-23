	package hw1;

public class Constants {
	
	// Run Configurations
	public static final int THREAD_NUM = 2;
	public static final int MODE_RUN_TIME = 10; // # of run loops for each mode
	public static boolean IS_FIBO_ON = false;

	// File Loader Constants
	public static final String CSV_SEPARATOR = ",";
	public static final String FILENAME_SPLITOR = "\\.";
	public static final String FILENAME_JOINER = ".";
	
	// Record Validation Constants
	public static final String VALID_TYPE = "TMAX";
	public static final String NUM_REGEX = "((-|\\+)?[0-9]+(\\.[0-9]+)?)+";
	
	// Debug Switches
	public static boolean PRINT_AVERAGES = false; // Print averages after each run
	public static boolean CHECK_RESULTS = false; // Check multi-thread averages with sequential averages after each run

}
