package topK;

/**
 * @author lixie
 * Node object used in treemap.
 */
public class TreeMapNode {

	private String pageName;
	private double pageRank;

	public TreeMapNode() {
		this.pageName = new String();
		this.pageRank = new Double(0);
	}

	public TreeMapNode(String pageName, double pageRank) {
		super();
		this.pageName = pageName;
		this.pageRank = pageRank;
	}

	public String getPageName() {
		return pageName;
	}

	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

	public double getPageRank() {
		return pageRank;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	@Override
	public String toString() {
		return "TreeMapNode [pageName=" + pageName + ", pageRank=" + pageRank + "]";
	}

}
