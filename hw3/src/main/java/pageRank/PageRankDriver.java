package pageRank;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import preProcess.PreProcessMapper;
import preProcess.PreProcessReducer;
import topK.TopKMapper;
import topK.TopKReducer;

/**
 * @author lixie
 * This is the driver class for wiki pagerank calculation
 * It includes three stages:
 * 1. Pre-process: take raw wiki page data, clean and parse page 
 * name and its adjacency list.
 * 2. PageRank: 10 times of pagerank and converge rate calculated 
 * for each page after each iteration.
 * 3. TopK: K pages with highest pagerank will be output with its
 * rank no., page name, page rank, and converge rate.
 */
public class PageRankDriver {

	// Global counters declarations
	public static String iterationNo = "iterationNo";
	public static String pageCount = "pageCount";
	public static String printConvergeRate = "printConvergeRate";
	public static String invalidPageCount = "invalidPageCount";
	public static String topK = "topK";

	// Setup Hadoop global counters
	public static enum globalCounters {
		iterationNo, pageCount, printConvergeRate, invalidPageCount, topK
	}

	/**
	 * @param input
	 * @param output
	 * @param conf
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static Job doPreProcessJob(String input, String output, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf, "PreProcess");
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(PreProcessMapper.class);
		job.setReducerClass(PreProcessReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Node.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
		return job;

	}

	/**
	 * @param input
	 * @param output
	 * @param pageCount
	 * @param iterationNo
	 * @param conf
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static Job doPageRankJob(String input, String output, long pageCount, int iterationNo, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {

		// Setup global counters to job conf
		conf.setInt(globalCounters.iterationNo.toString(), iterationNo);
		conf.setLong(globalCounters.pageCount.toString(), pageCount);

		Job job = Job.getInstance(conf, "PageRank_" + iterationNo);
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(PageRankMapper.class);
		job.setPartitionerClass(PageRankPartitioner.class);
		job.setSortComparatorClass(PageRankKeyComparator.class);
		job.setReducerClass(PageRankReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Node.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
		return job;

	}

	/**
	 * @param input
	 * @param output
	 * @param topK
	 * @param printConvergeRate
	 * @param conf
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static Job doTopKJob(String input, String output, int topK, boolean printConvergeRate, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {

		// Setup global counters to job conf
		conf.setInt(globalCounters.topK.toString(), topK);
		conf.setBoolean(globalCounters.printConvergeRate.toString(), printConvergeRate);

		Job job = Job.getInstance(conf, "TopK");
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
		return job;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// Debugging logger
		Logger logger = Logger.getLogger(PageRankDriver.class.getName());

		// Config M-R driver and parse args
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 4) {
			System.err.println("args : " + otherArgs.length);
			System.err.println("Usage: PageRank <in> <out> <topK> <printConvergeRate>");
			System.exit(1);
		}
		int topK = 100;
		boolean printConvergeRate = true;
		try {
			topK = Integer.valueOf(otherArgs[2]);
			printConvergeRate = Boolean.valueOf(otherArgs[3].toLowerCase().trim());
		} catch (Exception e) {
			System.err.println("args : " + otherArgs);
			System.err.println("Invalid <topK> -> int or <printConvergeRate> -> boolean");
			System.exit(1);
		}

		// Pre-process
		String input = otherArgs[0];
		String output = otherArgs[1] + "/preprocess";
		Job preProcessJob = doPreProcessJob(input, output, conf);

		// Read valid and invalid total page counts from pre-process job
		long pageCount = preProcessJob.getCounters().findCounter(globalCounters.pageCount).getValue();
		long invalidPageCount = preProcessJob.getCounters().findCounter(globalCounters.invalidPageCount).getValue();
		logger.log(Level.INFO, "Number of invalid page deleted: " + invalidPageCount + " out of " + (invalidPageCount + pageCount));

		// 10 Page Rank
		for (int iterationNo = 0; iterationNo < 10; iterationNo++) {
			input = output;
			output = otherArgs[1] + "/pagerank_" + iterationNo;
			Job pageRankJob = doPageRankJob(input, output, pageCount, iterationNo, conf);
		}

		// Top-K
		input = output;
		output = otherArgs[1] + "/topk";
		Job topKJob = doTopKJob(input, output, topK, printConvergeRate, conf);

	}

}
