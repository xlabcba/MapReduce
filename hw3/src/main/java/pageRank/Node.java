package pageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * @author lixie
 * Node object containing page info
 * and passed between mapper and reducer.
 */
public class Node implements Writable {
	
	private double pageRank;
	private List<String> adjacencyList;
	private boolean isNode; // if true, is real node; or else contribution value
	private double convergeRate; // |newPageRank - oldPageRank| / oldPageRank

	public Node() {
		this.pageRank = new Double(0);
		this.adjacencyList = new LinkedList<String>();
		this.isNode = true;
		this.convergeRate = new Double(1);
	}

	public Node(double pageRank, List<String> adjacencyList, boolean isNode, double convergeRate) {
		this.pageRank = pageRank;
		this.adjacencyList = adjacencyList;
		this.isNode = isNode;
		this.convergeRate = convergeRate;
	}

	public double getPageRank() {
		return pageRank;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	public List<String> getAdjacencyList() {
		return adjacencyList;
	}

	public void setAdjacencyList(List<String> adjacencyList) {
		this.adjacencyList = adjacencyList;
	}

	public boolean isNode() {
		return isNode;
	}

	public void setNode(boolean isNode) {
		this.isNode = isNode;
	}

	public double getConvergeRate() {
		return convergeRate;
	}

	public void setConvergeRate(double convergeRate) {
		this.convergeRate = convergeRate;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(pageRank);
		out.writeInt(adjacencyList.size());
		for (int i = 0; i < adjacencyList.size(); i++) {
			out.writeUTF(adjacencyList.get(i));
		}
		out.writeBoolean(isNode);
		out.writeDouble(convergeRate);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pageRank = in.readDouble();
		int size = in.readInt();
		adjacencyList = new ArrayList<String>();
		for (int i = 0; i < size; i++) {
			adjacencyList.add(in.readUTF());
		}
		isNode = in.readBoolean();
		convergeRate = in.readDouble();
	}

	@Override
	public String toString() {
		return "Node [pageRank=" + pageRank + ", adjacencyList=" + adjacencyList + ", isNode=" + isNode
				+ ", convergeRate=" + convergeRate + "]";
	}

}
