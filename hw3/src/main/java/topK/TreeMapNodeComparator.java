package topK;

import java.util.Comparator;

/**
 * @author lixie
 * Treemap Node Comparator 
 * to ensure the node will be firstly sorted in increasing order of page rank,
 * and then in increasing lexicographic order of page name if the page ranks
 * are same.
 */
public class TreeMapNodeComparator implements Comparator<TreeMapNode> {

	public int compare(TreeMapNode n1, TreeMapNode n2) {

		// Sorting in increasing order of page rank
		int cmp = (n1.getPageRank() == n2.getPageRank() ? 0 : (n1.getPageRank() > n2.getPageRank() ? 1 : -1));

		// If page ranks are same
		if (cmp == 0) {
			// sort in increasing lexicographic order of page name
			cmp = n1.getPageName().compareTo(n2.getPageName());
		}

		return cmp;

	}
}