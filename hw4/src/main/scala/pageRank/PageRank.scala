package pageRank

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext._;

import wikiParser.GraphGenerator;

object PageRank {
  
  def emitAdjList(adjList: List[String]) : List[(String, List[String])] = {
    adjList.map(node => (node, List[String]()))
  }
  
  def emitPageNode(node: String) : List[(String, List[String])] = {
    val pageNode = node.split("~~~")
    val pageName = pageNode(0)
    val outlinks = if (pageNode.size > 1) pageNode(1).split("~").toList else List[String]()
    List((pageName, outlinks)) ::: emitAdjList(outlinks)
  }
  
  def main(args: Array[String]) = {

    val inputs = "input/wikipedia-simple-html.bz2"

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("Page Rank")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Load input file as RDD
    val lines = sc.textFile(inputs)
    .map(line => GraphGenerator.createGraph(line))
    .filter(line => line != null) // Filter out invalid pages
    .persist()
    
    // Pre-Process on initial graph
    val graph = lines
    .flatMap(emitPageNode)
    .reduceByKey((adjList1, adjList2) => adjList1 ::: adjList2)
    .persist()
		
    // Save as text file
    graph.saveAsTextFile("output")
    
    //Stop the Spark context  
    sc.stop
  }
}