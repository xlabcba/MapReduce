package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hw1.NoLockCalculation.Worker;

public abstract class AbstractCalculation {

	public abstract void calculate(List<String> lines);
	
	public abstract void calculateAverages();
	
	public abstract void printSummary();
	
}
