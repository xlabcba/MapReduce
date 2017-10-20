package preProcess;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pageRank.Node;
import pageRank.PageRankDriver;

/**
 * @author lixie
 * Pre-process Reducer Class
 * to search for non-empty adjacency list for the node,
 * and emit as page node. Emit as dangling node if not found.
 * Either case increments total valid page count.
 */
public class PreProcessReducer extends Reducer<Text, Node, Text, Node> {

	@Override
	public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {

		Node resNode = new Node();

		for (Node node : values) {
			// Emit node if non-empty adjacency list for the page is found
			if (node.getAdjacencyList().size() != 0) {
				resNode.setAdjacencyList(node.getAdjacencyList());
				// Increment global counter of total page count
				context.getCounter(PageRankDriver.globalCounters.pageCount).increment(1);
				context.write(key, node);
				return;
			}
		}
		// Emit as dangling node
		// Increment global counter of total page count
		context.getCounter(PageRankDriver.globalCounters.pageCount).increment(1);
		context.write(key, resNode);
	}
}
