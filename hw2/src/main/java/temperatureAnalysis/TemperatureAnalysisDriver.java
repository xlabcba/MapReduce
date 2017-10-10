package temperatureAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import noCombiner.MapperWithNoCombiner;
import noCombiner.ReducerWithNoCombiner;
import noCombiner.StationRecordWritableWithNoCombiner;
import secondarySort.FirstPartitioner;
import secondarySort.GroupComparatorWithSecondarySort;
import secondarySort.KeyComparatorWithSecondarySort;
import secondarySort.CompositeKeyWithSecondarySort;
import secondarySort.MapperWithSecondarySort;
import secondarySort.ReducerWithSecondarySort;
import secondarySort.StationRecordWritableWithSecondarySort;
import combiner.MapperWithCombiner;
import combiner.ReducerWithCombiner;
import combiner.Combiner;
import combiner.StationRecordWritableWithCombiner;
import inMapperCombining.MapperWithInMapperComb;
import inMapperCombining.ReducerWithInMapperComb;
import inMapperCombining.StationRecordWritableWithInMapperComb;

/**
 * @author lixie
 * This class is an implementation of multi-version
 * weather data analysis. Given path to weather data records
 * and running mode, it will load data and calculating per-
 * station temperature average in the corresponding mode.
 * The mode can be nocombiner, combiner, inmappercomb, 
 * and secondarysort
 */
public class TemperatureAnalysisDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		// Config M-R driver and parse args
	    Configuration conf = new Configuration();
	    conf.set("mapred.textoutputformat.separator", ", ");
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 3) {
		  System.err.println("Usage: " + otherArgs.length);
	      System.err.println("Usage: TemperatureAnalysis ${mode} <in> <out>");
	      System.exit(1);
	    }
	    Job job = new Job(conf, "TemperatureAnalysis");
	    job.setJarByClass(TemperatureAnalysisDriver.class);
	    
	    // Setup job based on mode given
	    if (otherArgs[0].equalsIgnoreCase("nocombiner")) {
	    	job.setMapperClass(MapperWithNoCombiner.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(StationRecordWritableWithNoCombiner.class);
		    job.setReducerClass(ReducerWithNoCombiner.class);
	    } else if (otherArgs[0].equalsIgnoreCase("combiner")){
	    	job.setMapperClass(MapperWithCombiner.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(StationRecordWritableWithCombiner.class);
		    job.setCombinerClass(Combiner.class);
		    job.setReducerClass(ReducerWithCombiner.class);
	    } else if (otherArgs[0].equalsIgnoreCase("inmappercomb")) {
	    	job.setMapperClass(MapperWithInMapperComb.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(StationRecordWritableWithInMapperComb.class);
		    job.setReducerClass(ReducerWithInMapperComb.class);
	    } else if (otherArgs[0].equalsIgnoreCase("secondarysort")) {
	    	job.setMapperClass(MapperWithSecondarySort.class);
	    	job.setMapOutputKeyClass(CompositeKeyWithSecondarySort.class);
	    	job.setMapOutputValueClass(StationRecordWritableWithSecondarySort.class);
	    	job.setPartitionerClass(FirstPartitioner.class);
	    	job.setSortComparatorClass(KeyComparatorWithSecondarySort.class);
	    	job.setGroupingComparatorClass(GroupComparatorWithSecondarySort.class);
	    	job.setReducerClass(ReducerWithSecondarySort.class);
	    	
	    } else {
		      System.err.println("Usage: ${mode} can only be (nocombiner/combiner/inmappercomb)");
		      System.exit(1);
	    }
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
