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

	public Node() {
		pageRank = new Double(0);
		adjacencyList = new ArrayList<String>();
	}

	public Node(double pageRank, List<String> adjacencyList) {
		this.pageRank = pageRank;
		this.adjacencyList = adjacencyList;
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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(pageRank);
		out.writeInt(adjacencyList.size());
		for (int i = 0; i < adjacencyList.size(); i++) {
			out.writeUTF(adjacencyList.get(i));
		}
	}

	public void readFields(DataInput in) throws IOException {
		pageRank = in.readDouble();
		int size = in.readInt();
		adjacencyList = new ArrayList<String>();
		for (int i = 0; i < size; i++) {
			adjacencyList.add(in.readUTF());
		}
	}

	@Override
	public String toString() {
		return "Node [pageRank=" + pageRank + ", adjacencyList=" + adjacencyList + "]";
	}
	
}
