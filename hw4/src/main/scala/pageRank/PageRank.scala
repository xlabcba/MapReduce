package pageRank

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext._;

import wikiParser.GraphGenerator;

object PageRank {
    	
  implicit val sortTupleByDoubleAndString = new Ordering[(String, Double)] {
    override def compare(a: (String, Double), b: (String, Double)) = {
      if (a._2 > b._2) {
        1
      } else if (a._2 < b._2) {
        -1
      } else {
        b._1.compareTo(a._1)
      }
    }
  }
  
  def emitPageNode(node: String) : List[(String, List[String])] = {
    val pageArray = node.split("~~~")
    val pageName = pageArray(0)
    val outlinks = if (pageArray.size > 1) pageArray(1).split("~").toList else List[String]()
    val pageNode = List((pageName, outlinks))
    val adjNodes = outlinks.map(node => (node, List[String]()))
    pageNode ::: adjNodes
  }
  
  def distributeContribution(node: (String, (Double, List[String]))) : List[(String, (Double, List[String]))] = {
    val pageName = node._1
    val adjList = node._2._2
    val contribution = node._2._1 / adjList.length
    val nodeStructure = List((pageName, (0.0, adjList)))
    val contributionList = node._2._2.map(adjNode => (adjNode, (contribution, List[String]())))
    nodeStructure ::: contributionList
  }
  
  def accumulateContribution(node1: (Double, List[String]), node2: (Double, List[String])) : (Double, List[String]) = {
    (node1._1 + node2._1, node1._2 ::: node2._2)
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
    
    // Count total valid page and add initial pagerank
    val pageCount = graph.count()
    println(s"[DEBUG] PAGE COUNT: ${pageCount}")
    val initPageRank = 1.0 / pageCount
    var pageNodes = graph
    .map(pageNode => (pageNode._1, (initPageRank, pageNode._2)))
    
    // 10 times of Pagerank job
    for ( i <- 1 to 10 ) {
      println(s"[DEBUG] LOOP ${i}")
      
      // Calculate delta sum (pagerank sum of dangling nodes)
      val deltaSum = pageNodes
      .filter(node => node._2._2.length == 0)
      .aggregate(0.0)((curSum, node) => curSum + node._2._1, (sum1, sum2) => sum1 + sum2)
      println(s"[DEBUG] DELTA SUM: ${deltaSum}")
        
      // Distribute  and accumulate contributions 
      pageNodes = pageNodes
      .flatMap(distributeContribution)
      .reduceByKey(accumulateContribution)
      .mapValues(t => (0.15 / pageCount + 0.85 * (t._1 + deltaSum / pageCount), t._2))     
      
      val pageRankSum = pageNodes
      .aggregate(0.0)((curSum, node) => (curSum + node._2._1), (sum1, sum2) => sum1 + sum2)
      println(s"[DEBUG] PAGE RANK SUM: ${pageRankSum}")
    }
    
    // Top K job
    val topK = pageNodes
    .map(node => (node._1, node._2._1))
    .top(100)
    
    // Save as text file
    sc.parallelize(topK, 1).saveAsTextFile("output")
    
    //Stop the Spark context  
    sc.stop
  }
}