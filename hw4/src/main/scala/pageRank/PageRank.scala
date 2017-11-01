package pageRank

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext._;

import wikiParser.GraphGenerator;

object PageRank {
  
  case class PageNode(pageRank:Double, adjList:List[String])
  	
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
  
  def distributeContribution(node: (String, (Double, List[String]))) : List[(String, Double)] = {
    val pageName = node._1
    val adjList = node._2._2
    val contribution = node._2._1 / adjList.length
    val nodeStructure = List((pageName, 0.0))
    val contributionList = adjList.map(adjNode => (adjNode, contribution))
    nodeStructure ::: contributionList
  }
  
  def accumulateContribution(rank1: Double, rank2: Double) : Double = {
    rank1 + rank2
  }
  
  def main(args: Array[String]) = {
    
    // Parse args
    val input = args(0)
    val output = args(1)
    val k = Integer.valueOf(args(2))
    val debug = if (args(3).equalsIgnoreCase("true")) true else false

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("Page Rank")
    val sc = new SparkContext(conf)

    // Load input file as RDD
    // and do Pre-process job
    val graph = sc.textFile(input)
    .map(line => GraphGenerator.createGraph(line))
    .filter(line => line != null) // Filter out invalid pages
    .flatMap(emitPageNode)
    .reduceByKey((adjList1, adjList2) => adjList1 ::: adjList2)
    .persist()
    
    // Count total valid page and add initial pagerank
    val pageCount = graph.count()
    val initPageRank = 1.0 / pageCount
    var ranks = graph
    .map(node => (node._1, initPageRank))
    .persist()
    
    // Debug logging
    if (debug) {
      println(s"[DEBUG] PAGE COUNT: ${pageCount}")
      val graphUseMemory = graph.getStorageLevel.useMemory
      println(s"[DEBUG] GRAPH USES MEMORY: ${graphUseMemory}")
      val ranksUseMemory = ranks.getStorageLevel.useMemory
      println(s"[DEBUG] RANKS USES MEMORY: ${ranksUseMemory}")
    }
    
    // 10 times of Pagerank job
    for ( i <- 1 to 10 ) {
      // Debug logging
      if (debug) {
        println(s"[DEBUG] LOOP ${i}")
      }
      
      // Join ranks with graph
      val pageNodes = ranks.join(graph)
  
      // Calculate delta sum (pagerank sum of dangling nodes)
      val deltaSum = pageNodes
      .filter(node => node._2._2.length == 0)
      .aggregate(0.0)((curSum, node) => curSum + node._2._1, (sum1, sum2) => sum1 + sum2)
        
      ranks.unpersist()
      
      // Distribute  and accumulate contributions 
      ranks = pageNodes
      .flatMap(distributeContribution)
      .reduceByKey(accumulateContribution)
      .mapValues(rank => 0.15 / pageCount + 0.85 * (rank + deltaSum / pageCount))
      .persist()
      
      // Debug logging
      if (debug) {
        println(s"[DEBUG] DELTA SUM: ${deltaSum}")
        val loopRanksUseMemory = ranks.getStorageLevel.useMemory
        println(s"[DEBUG] IN-LOOP NODES USES MEMORY: ${loopRanksUseMemory}")
        val pageRankSum = ranks
        .aggregate(0.0)((curSum, node) => (curSum + node._2), (sum1, sum2) => sum1 + sum2)
        println(s"[DEBUG] PAGE RANK SUM: ${pageRankSum}")
      }
    }
    
    // Top K job
    val topK = ranks.top(k)
    
    // Save as text file
    sc.parallelize(topK, 1).saveAsTextFile(output)
    Thread.sleep(1000000000)
    
    //Stop the Spark context  
    sc.stop
  }
}