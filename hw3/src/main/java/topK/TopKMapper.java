package topK;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pageRank.Node;
import pageRank.PageRankDriver;

/**
 * @author lixie
 * TopK Mapper Class
 * to find local top k page nodes within current map tasks,
 * and emit as final top k candidates.
 * Nodes are compared first in decreasing order of pagerank,
 * and then in increasing lexicographic order if the pageranks are same.
 */
public class TopKMapper extends Mapper<Text, Node, NullWritable, Text> {

	// Constants
	private static final String SPLITOR = "~";
	
	// Declare class-level variables
	private TreeMap<TreeMapNode, String> topKMap;
	private int topK;
	private boolean printConvergeRate;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		// Initialize treemap
		this.topKMap = new TreeMap<TreeMapNode, String>(new TreeMapNodeComparator());
		// Read global counters
		this.topK = context.getConfiguration().getInt(PageRankDriver.topK, 100);
		this.printConvergeRate = context.getConfiguration().getBoolean(PageRankDriver.printConvergeRate, false);
	}

	@Override
	public void map(Text key, Node value, Context context) throws IOException, InterruptedException {

		// Put new node into treemap
		TreeMapNode node = new TreeMapNode(key.toString(), value.getPageRank());
		String outStr = key.toString() + SPLITOR + value.getPageRank();
		if (printConvergeRate) {
			outStr += (SPLITOR + (Math.round(value.getConvergeRate() * 100000) / 1000.0) + "%");
		}
		topKMap.put(node, outStr);

		// If more than K, remove the smallest
		if (topKMap.size() > topK) {
			topKMap.remove(topKMap.firstKey());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (String outStr : topKMap.values()) {
			context.write(NullWritable.get(), new Text(outStr));
		}
	}

}
