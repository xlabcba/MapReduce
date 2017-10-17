package topK;


import java.util.Comparator;

public class TreeMapNodeComparator implements Comparator<TreeMapNode> {

    public int compare(TreeMapNode n1, TreeMapNode n2) {
    	
    	int cmp = (n1.pageRank == n2.pageRank ? 0 : (n1.pageRank > n2.pageRank ? 1 : -1));
    	
    	if (cmp == 0) {
    		cmp = n1.pageName.compareTo(n2.pageName);
    	}
    	
    	return cmp;

    }
}