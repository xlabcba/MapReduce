package pageRank

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import wikiParser.GraphGenerator;

object PageRank {
  
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
    .filter(line => line != null)
    .persist()
		
		// Save as text file
    lines.saveAsTextFile("output")
    
		//Stop the Spark context  
		sc.stop
	}
}