package pageRank;

import java.io.IOException;
import java.util.LinkedList;
// import java.util.logging.Level;
// import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author lixie
 * Page Rank Mapper Class
 * to distribute current page rank of the node equally
 * along its outlinks, and send out page rank for delta
 * correction if the node is dangling.
 */
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
		// this.logger.log(Level.INFO, "Mapper Iteration No." + iterationNo + ";
		// pageCount: " + pageCount + ";");
	}

	@Override
	public void map(Text key, Node value, Context context) throws IOException, InterruptedException {

		// Adjust page rank for first time run
		if (iterationNo == 0) {
			value.setPageRank(1.0 / pageCount);
		}

		// Pass along graph structure
		context.write(key, value);

		// Send contribution if not dangling node
		if (value.getAdjacencyList().size() != 0) {
			// Compute contribution to send along outlinks
			double contribution = value.getPageRank() / value.getAdjacencyList().size();
			// Sent contribution along outlinks
			for (String outlinkName : value.getAdjacencyList()) {
				Node contributionNode = new Node(contribution, new LinkedList<String>(), false, new Double(1));
				context.write(new Text(outlinkName), contributionNode);
			}
		} else { // Dangling node
			// Send page rank as dummy node to each reduce task
			Node dummyNode = new Node(value.getPageRank(), new LinkedList<String>(), false, new Double(1));
			for (int i = 0; i < context.getNumReduceTasks(); i++) {
				context.write(new Text("~" + i), dummyNode);
			}
		}

	}
}
