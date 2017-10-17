package topK;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pageRank.Node;

public class TopKMapper extends Mapper<Text, Node, NullWritable, Text> {
	
	// Constants
	private static final int K = 100;
	
	private TreeMap<TreeMapNode, String> topKMap;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		this.topKMap = new TreeMap<TreeMapNode, String>(new TreeMapNodeComparator());
    }
	
	@Override
	public void map(Text key, Node value, Context context) throws IOException, InterruptedException {
		
		TreeMapNode node = new TreeMapNode(key.toString(), value.pageRank);
		String outStr = key.toString() + "," + value.pageRank;
		topKMap.put(node, outStr);

		if (topKMap.size() > K) {
			topKMap.remove(topKMap.firstKey());
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,InterruptedException {
		for (String outStr : topKMap.values()) {
			context.write(NullWritable.get(), new Text(outStr));
		}
	}

}
