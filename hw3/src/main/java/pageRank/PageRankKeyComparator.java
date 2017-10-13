package pageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PageRankKeyComparator extends WritableComparator {

	protected PageRankKeyComparator() {
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Text k1 = (Text) w1;
		Text k2 = (Text) w2;

		if ((!k1.toString().contains("~") && !k2.toString().contains("~")) 
			|| (k1.toString().contains("~") && k2.toString().contains("~"))) {
			return k1.toString().compareTo(k2.toString());
		} else {
			return (k1.toString().contains("~") ? -1 : 1);
		}
	}

}