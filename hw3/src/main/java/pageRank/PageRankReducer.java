package pageRank;

import java.io.IOException;
// import java.util.logging.Level;
// import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author lixie
 * Page Rank Reducer Class
 * to  accumulate pagerank of dangling node into delta sum
 * for dangling correction, and adding up inlink contributions
 * and calculate new page rank, as well as converge rate for the node.
 */
public class PageRankReducer extends Reducer<Text, Node, Text, Node> {

	// Constants
	private static final double ALPHA = 0.15;

	// Global counters
	private long pageCount;
	private double deltaSum;

	// Debugging values
	// private double pageRankSum;
	// private int iterationNo;
	// private Logger logger;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		// Read global counters and validate them
		this.pageCount = context.getConfiguration().getLong(PageRankDriver.pageCount, -100);
		if (this.pageCount == -100) {
			throw new Error("Reducer failed to pass global counters!");
		}
		this.deltaSum = new Double(0);

		// Debugging values
		// this.iterationNo =
		// context.getConfiguration().getInt(PageRankDriver.iterationNo, -100);
		// this.pageRankSum = new Double(0);
		// this.logger = Logger.getLogger(PageRankReducer.class.getName());
		// this.logger.log(Level.INFO, "Setup in Iteration No." +
		// this.iterationNo + "; pageCount = " + this.pageCount + ";");
	}

	@Override
	public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {

		// If dummy node from dangling node
		// update deltaSum for dangling correction
		if (key.toString().contains("~")) {
			double newDeltaSum = new Double(0);
			for (Node node : values) {
				newDeltaSum += node.getPageRank();
			}
			this.deltaSum = newDeltaSum;
			
			// Debugging log
			// this.logger.log(Level.INFO, "deltaSum: " + this.deltaSum);
		} else {
			// Initialize total inlinks contribution and node
			double inlinkContribution = new Double(0);
			Node resNode = new Node();
			double oldPageRank;
			double newPageRank;

			// Iterate over input list
			for (Node value : values) {
				if (value.isNode()) {
					// Node found to recover graph structure
					resNode.setAdjacencyList(value.getAdjacencyList());
					resNode.setPageRank(value.getPageRank());
				} else {
					// Inlink contribution found to accumulate new page rank
					inlinkContribution += value.getPageRank();
				}
			}

			// Dangling correction
			inlinkContribution += this.deltaSum / this.pageCount;

			// Compute page rank and converge rate, then emit
			oldPageRank = resNode.getPageRank();
			newPageRank = (ALPHA / this.pageCount) + ((1.0 - ALPHA) * inlinkContribution);
			resNode.setConvergeRate(Math.abs(newPageRank - oldPageRank) / oldPageRank);
			resNode.setPageRank(newPageRank);
			context.write(key, resNode);

			// Debugging values
			// pageRankSum += resNode.pageRank;
			// this.logger.log(Level.INFO, "new pageRank: " + resNode.pageRank);
		}
	}

	// Debugging log
	// @Override
	// public void cleanup(Context context) throws IOException,
	// InterruptedException {
	// this.logger.log(Level.INFO, "Iteration No." + this.iterationNo + ":
	// PageRankSum = " + this.pageRankSum);
	// }
}
