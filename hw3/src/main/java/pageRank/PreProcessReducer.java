package pageRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreProcessReducer extends Reducer<Text, Node, Text, Node> {
	
	private Logger logger = Logger.getLogger(PreProcessReducer.class.getName());

	@Override
	public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
		
		int count = 0;
		
		for (Node node : values) {
			if (count == 0) {
				context.write(key, node);
			}
			count++;
		}
		
		logger.log(Level.INFO, key.toString() + " count: " + count);
		
	}
}
