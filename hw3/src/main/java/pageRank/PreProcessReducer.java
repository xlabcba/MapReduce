package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreProcessReducer extends Reducer<Text, Node, Text, Node> {
	

	@Override
	public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
		
		Node resNode = new Node();
		
		for (Node node : values) {
			if (node.adjacencyList.size() != 0) {
				resNode.adjacencyList = node.adjacencyList;
				context.getCounter(PageRankDriver.globalCounters.pageCount).increment(1);
				context.write(key, node);
				return;
			}
		}
		context.getCounter(PageRankDriver.globalCounters.pageCount).increment(1);
		context.write(key, resNode);
	}
}
