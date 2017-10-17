package pageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class PageRankDriver {

	public static String iterationNo = "iterationNo";
	public static String pageCount = "pageCount";

	// Setup Hadoop global counters
	public static enum globalCounters {
		iterationNo, pageCount
	}

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
	
	public static Job doPageRankJob(String input, String output, 
			long pageCount, int iterationNo, Configuration conf)
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
	
    public static Job doTopKJob(String input, String output, Configuration conf) 
    		throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(conf, "TopK");
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
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
		String input = otherArgs[0];
		String output = otherArgs[1] + "/preprocess";
		Job preProcessJob = doPreProcessJob(input, output, conf);
	
        long pageCount = preProcessJob.getCounters().findCounter(globalCounters.pageCount).getValue();

        // Page Rank
        ;
        for (int iterationNo = 0; iterationNo < 10; iterationNo++) {
            input = output;
            output = otherArgs[1] + "/pagerank_" + iterationNo;          
            Job pageRankJob = doPageRankJob(input, output, pageCount, iterationNo, conf);
        }

		// Top-K
        input = output;
        output = otherArgs[1] + "/topk";     
        Job topKJob = doTopKJob(input, output, conf);
        
	}

}
