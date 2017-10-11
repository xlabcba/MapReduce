package pageRank;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRankDriver {

	public static Job doPreProcessJob(String inputPath, String outputPath, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {

		Job job = new Job(conf, "PageRank");
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(PreProcessMapper.class);
		job.setReducerClass(PreProcessReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Node.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
		return job;

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// Config M-R driver and parse args
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("args : " + otherArgs.length);
		    System.err.println("Usage: PageRank <in> <out>");
			System.exit(1);
		}

		// Pre-process
		Job preProcessJob = doPreProcessJob(otherArgs[0], otherArgs[1], conf);

		// Page Rank

		// Top-K

	}

}
