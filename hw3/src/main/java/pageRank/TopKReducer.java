package pageRank;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKReducer extends Reducer<NullWritable, Text, Text, DoubleWritable> {
	
	// Constants
	private static final String SPLITOR = ",";

	private TreeMap<TreeMapNode, String> topKMap = new TreeMap<TreeMapNode, String>(new TreeMapNodeComparator());

	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for (Text value : values) {
			String[] entry = value.toString().split(SPLITOR);
			TreeMapNode node = new TreeMapNode(entry[0], Double.valueOf(entry[1]));
			topKMap.put(node, value.toString());
			
			if (topKMap.size() > 100) {
				topKMap.remove(topKMap.firstKey());
			}
		}
		
		for (String outStr : topKMap.descendingMap().values()) {
			String[] entry = outStr.split(SPLITOR);
			context.write(new Text(entry[0]), new DoubleWritable(Double.valueOf(entry[1])));
		}
	}
	
}
