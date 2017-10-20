package pageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author lixie
 * Page Rank Key Comparator class 
 * to ensure each reduce task will sum up pagerank 
 * of all dangling nodes before all of other normal
 * reduce calls.
 */
public class PageRankKeyComparator extends WritableComparator {

	protected PageRankKeyComparator() {
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text k1 = (Text) w1;
		Text k2 = (Text) w2;

		// Normal comparision if both are not dummy node, or both are dummy node
		if ((!k1.toString().contains("~") && !k2.toString().contains("~"))
				|| (k1.toString().contains("~") && k2.toString().contains("~"))) {
			return k1.toString().compareTo(k2.toString());
		} else { // The dummy node comes first
			return (k1.toString().contains("~") ? -1 : 1);
		}
	}

}