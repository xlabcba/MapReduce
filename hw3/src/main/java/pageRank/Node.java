package pageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class Node implements Writable {

	public double pageRank;
	public List<String> adjacencyList;
	public boolean isNode; // if true, is real node; or else contribution value

	public Node() {
		this.pageRank = new Double(0);
		this.adjacencyList = new ArrayList<String>();
		this.isNode = true;
	}

	public Node(double pageRank, List<String> adjacencyList, boolean isNode) {
		this.pageRank = pageRank;
		this.adjacencyList = adjacencyList;
		this.isNode = isNode;
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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(pageRank);
		out.writeInt(adjacencyList.size());
		for (int i = 0; i < adjacencyList.size(); i++) {
			out.writeUTF(adjacencyList.get(i));
		}
		out.writeBoolean(isNode);
	}

	public void readFields(DataInput in) throws IOException {
		pageRank = in.readDouble();
		int size = in.readInt();
		adjacencyList = new ArrayList<String>();
		for (int i = 0; i < size; i++) {
			adjacencyList.add(in.readUTF());
		}
		isNode = in.readBoolean();
	}

	@Override
	public String toString() {
		return "Node [pageRank=" + pageRank + ", adjacencyList=" + adjacencyList + ", isNode=" + isNode + "]";
	}
	
}
