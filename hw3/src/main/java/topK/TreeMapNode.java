package topK;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TreeMapNode {
	
	public String pageName;
	public double pageRank;
	
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
