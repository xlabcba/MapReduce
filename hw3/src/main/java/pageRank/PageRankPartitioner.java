package pageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author lixie
 * Page Rank Partitioner Class
 * to ensure each reduce task will receive exactly one
 * page rank of each dangling node for accumulating delta sum
 * for dangling correction.
 */
public class PageRankPartitioner extends Partitioner<Text, Node> {

	@Override
	public int getPartition(Text key, Node value, int numPartitions) {
		// If dummy node, has format of "~i" where i is [0, numPartitions)
		// Just return i to send to corresp. reduce task. Add modular for safety.
		if (key.toString().contains("~")) {
			return Integer.valueOf(key.toString().replace("~", "")) % numPartitions;
		} else {
			return (key.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

}
