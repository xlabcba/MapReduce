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
import combiner.MapperWithCombiner;
import combiner.ReducerWithCombiner;
import combiner.Combiner;
import combiner.StationRecordWritableWithCombiner;
import inMapperCombining.MapperWithInMapperComb;
import inMapperCombining.ReducerWithInMapperComb;
import inMapperCombining.StationRecordWritableWithInMapperComb;

public class TemperatureAnalysisDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
	    Configuration conf = new Configuration();
	    conf.set("mapred.textoutputformat.separator", ", ");
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 4) {
		  System.err.println("Usage: " + otherArgs.length);
	      System.err.println("Usage: TemperatureAnalysis ${mode} <in> <out>");
	      System.exit(1);
	    }
	    Job job = new Job(conf, "TemperatureAnalysis");
	    job.setJarByClass(TemperatureAnalysisDriver.class);
	    
	    if (otherArgs[1].equalsIgnoreCase("nocombiner")) {
	    	job.setMapperClass(MapperWithNoCombiner.class);
		    job.setReducerClass(ReducerWithNoCombiner.class);
		    job.setMapOutputValueClass(StationRecordWritableWithNoCombiner.class);
	    } else if (otherArgs[1].equalsIgnoreCase("combiner")){
	    	job.setMapperClass(MapperWithCombiner.class);
		    job.setReducerClass(ReducerWithCombiner.class);
		    job.setCombinerClass(Combiner.class);
		    job.setMapOutputValueClass(StationRecordWritableWithCombiner.class);
	    } else if (otherArgs[1].equalsIgnoreCase("inmappercomb")) {
	    	job.setMapperClass(MapperWithInMapperComb.class);
		    job.setReducerClass(ReducerWithInMapperComb.class);
		    job.setMapOutputValueClass(StationRecordWritableWithInMapperComb.class);
	    } else {
		      System.err.println("Usage: ${mode} can only be (nocombiner/combiner/inmappercomb)");
		      System.exit(1);
	    }
	    job.setMapOutputKeyClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
