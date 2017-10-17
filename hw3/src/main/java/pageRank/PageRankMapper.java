package pageRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Node, Text, Node> {
	
	// Global Counters
	private long pageCount;
	private int iterationNo;
	
	// Debugging Values
	// private Logger logger;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		// Read global counters and validate them
        this.pageCount = context.getConfiguration().getLong(PageRankDriver.pageCount, -100);
        this.iterationNo = context.getConfiguration().getInt(PageRankDriver.iterationNo, -100);
        if (this.pageCount == -100 || this.iterationNo == -100) {
        	 throw new Error("Mapper failed to pass global counters!");
        }
        // Initialize debugging values
        // this.logger = Logger.getLogger(PageRankMapper.class.getName());
        // this.logger.log(Level.INFO, "Mapper Iteration No." + iterationNo + "; pageCount: " + pageCount + ";");
    }

	@Override
	public void map(Text key, Node value, Context context) throws IOException, InterruptedException {
		
        // Adjust page rank for first time run
        if (iterationNo == 0) {
        	value.pageRank = 1.0 / pageCount;
        }
        
		// Pass along graph structure
		context.write(key, value);
		
		// Send contribution if not dangling node, 
		// or else increment to dangling mass accumulator
		if (value.adjacencyList.size() != 0) {
			// Compute contribution to send along outlinks
			double contribution = value.pageRank / value.adjacencyList.size();
			// Sent contribution along outlinks
			for (String outlinkName : value.adjacencyList) {
				Node contributionNode = new Node(contribution, new LinkedList<String>(), false);
				context.write(new Text(outlinkName), contributionNode);
			}	
		} else {
			// Increment page rank to total dangling mass accumulator
			Node dummyNode = new Node(value.pageRank, new LinkedList<String>(), false);
			for (int i = 0; i < context.getNumReduceTasks(); i++) {
				context.write(new Text("~" + i), dummyNode);	
			}
		}
				
	}
}
