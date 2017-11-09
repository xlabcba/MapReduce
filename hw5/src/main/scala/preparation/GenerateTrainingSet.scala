package preparation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import fileLoader.LoadMultiStack

object GenerateTrainingSet {
  
  def main(args: Array[String]) = {
    
    // Parse args
    // val input = args(0)
    // val output = args(1)
    val input = "input/1_image.tiff"
    
    //Start the Spark context
    val conf = new SparkConf()
    .setAppName("Image Preparation")
    val sc = new SparkContext(conf)
    
    val image = LoadMultiStack.load(input, 512, 512, 60) 
    
  }
}