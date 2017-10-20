package topK;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pageRank.PageRankDriver;

/**
 * @author lixie
 * TopK Reducer Class
 * to find final top K nodes from the local top K candidates.
 * The nodes are compared with same order as defined in comparator.
 */
public class TopKReducer extends Reducer<NullWritable, Text, IntWritable, Text> {

	// Constants
	private static final String SPLITOR = "~";

	// Declare class-level variables
	private int topK;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		// Read global counter
		this.topK = context.getConfiguration().getInt(PageRankDriver.topK, 100);
	}

	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// Initialize treemap
		TreeMap<TreeMapNode, String> topKMap = new TreeMap<TreeMapNode, String>(new TreeMapNodeComparator());

		// For each candidate node in input list
		for (Text value : values) {
			// Add candidate node into treemap
			String[] entry = value.toString().split(SPLITOR);
			TreeMapNode node = new TreeMapNode(entry[0], Double.valueOf(entry[1]));
			String outStr = "PageName: " + entry[0] + ", PageRank: " + entry[1];
			if (entry.length == 3) {
				outStr += (", ConvergeRate: " + entry[2]);
			}
			topKMap.put(node, outStr);

			// If more than K, remove the smallest
			if (topKMap.size() > topK) {
				topKMap.remove(topKMap.firstKey());
			}
		}

		// Emit top K each node with its rank number, page name, page rank, 
		// and converge rate, in decreasing order of the page rank.
		int index = 1;
		for (String outStr : topKMap.descendingMap().values()) {
			context.write(new IntWritable(index++), new Text(outStr));
		}
	}

}
