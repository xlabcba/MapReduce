package pageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PageRankPartitioner extends Partitioner<Text, Node> {

	@Override
	public int getPartition(Text key, Node value, int numPartitions) {
		if (key.toString().contains("~")) {
			return Integer.valueOf(key.toString().replace("~", "")) % numPartitions;
		} else {
			return (key.toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

}
