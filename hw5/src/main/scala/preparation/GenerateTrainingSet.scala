package preparation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.udf

import fileLoader.LoadMultiStack

object GenerateTrainingSet {
  
  def toMatrixCoord(index: Int) : (Int, Int, Int, Int) = {
    val xDim = 512; val yDim = 512; val zDim = 33;
    val z = index / (xDim * yDim)
    val remain = index - (z * xDim * yDim)
    val y = remain / xDim;
    val x = remain % xDim;
    (index, x, y, z)
  }
  
  def toArrayIdx(x: Int, y: Int, z: Int) : Int = {
    val xDim = 512; val yDim = 512; val zDim = 33;
    z * xDim * yDim + y * xDim + x
  }
  
  def toLabel(value: Byte) : Int = {
    if (value.toInt <= 1) {
      return 1
    } else {
      if (value.toInt > 3) {
        return 0
      } else {
        return -1
      }
    }
  }
  
  def generateNeighbors(i: Int, x: Int, y: Int, z: Int) : List[(Int, Int)] = {
    val xNum = 3; val yNum = 3; val zNum = 3;
    val xDim = 512; val yDim = 512; val zDim = 33;
    val res = new Array[(Int, Int)](xNum * yNum * zNum)
    var cur = 0
    for (dx <- -(xNum / 2) to (xNum / 2)) {
      for (dy <- -(yNum / 2) to (yNum / 2)) {
        for (dz <- -(zNum / 2) to (zNum / 2)) {
          val (nx, ny, nz) = (x + dx, y + dy, z + dz)
          if (nx < 0 || nx >= xDim || ny < 0 || ny >= yDim || nz < 0 || nz >= zDim) {
            return List((-1, i))
          }
          val neighborIdx = nz * xNum * yNum + ny * xNum + nx
          // val currIdx = (zNum / 2 + dz) * xNum * yNum + (yNum / 2 + dy) * xNum + (xNum / 2 + dx)
          res(cur) = (neighborIdx, i)
          cur += 1
        }
      }
    }
    return res.toList
  }
      
  def main(args: Array[String]) = {
    
    // Parse args
    // val input = args(0)
    // val output = args(1)
    val inputImage = "input/2_image.tiff"
    val inputDist = "input/2_dist.tiff"
    val output = "output"
    
    //Start the Spark context
    val conf = new SparkConf()
    .setAppName("Image Preparation")
    //.set("spark.network.timeout", "3300s")
    .set("spark.executor.memory", "8g")
    //.set("spark.executor.heartbeatInterval", "33s")
    val sc = new SparkContext(conf)
    
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val imageArray = LoadMultiStack.load(inputImage, 512, 512, 33)
    val imageMatrix = sc.parallelize(imageArray).zipWithIndex
    .map{case(brightness, index) => (index.toInt, brightness.toInt)}
    .filter{case(index, brightness) => brightness != 0}
    // .map{case(brightness, index) => toMatrixValue(brightness, index)}
    // .toDF("I", "X", "Y", "Z", "Brightness")
    .toDF("ImageIdx", "Brightness")
    .persist(StorageLevel.MEMORY_AND_DISK)
    
    imageMatrix.show()
        
    // val imageMap = sc.broadcast(imageMatrix.collectAsMap.toMap)
    
    // imageMatrix.rdd.saveAsTextFile(output)    
 
    val distArray = LoadMultiStack.load(inputDist, 512, 512, 33)
    val labelMatrix = sc.parallelize(distArray).zipWithIndex
    .map{case(distance, index) => (index.toInt, toLabel(distance))}
    .filter{case(index, label) => label != -1}
    .persist(StorageLevel.MEMORY_AND_DISK)
    
    val labelDF = labelMatrix.toDF("LabelIdx", "Label")
    
    labelDF.show()
    
    val neighborMatrix = labelMatrix
    .map{case(index, label) => toMatrixCoord(index)}
    .flatMap{case(i, x, y, z) => generateNeighbors(i, x, y, z)}
    .filter{case(neighborIdx, index) => neighborIdx != -1}
    .toDF("ImageIdx", "LabelIdx")
    .persist(StorageLevel.MEMORY_AND_DISK)
    
    neighborMatrix.show()
    
    //val immediateRes = neighborMatrix
    //.map{case(neighborIdx, index) => (index, List(imageMap.value.getOrElse(neighborIdx, 0)))}
    //  val x = List(imageMap.value.getOrElse(neighborIdx, 0)) 
    //  (index, x)}
    //.reduceByKey(_ ::: _)
    
    val immediateRes = neighborMatrix.join(imageMatrix, Seq("ImageIdx"), "left_outer")
    .select("LabelIdx", "Brightness")
    .na.fill(0, Seq("Brightness"))
    .persist(StorageLevel.MEMORY_AND_DISK)
    
    immediateRes.show()
    
    //val aggregateDataFrames = udf((x: Int, y: Int) => Seq(x,y))
    
    val neighborBrightness = immediateRes     
    .groupBy("LabelIdx")
    .agg(collect_list($"Brightness"))
    //.withColumn("list", aggregateDataFrames(immediateRes("Brightness")))
    //.select("OriginalIdx","list")
    
    neighborBrightness.show()
    
    val trainingRecord = labelDF
    .join(neighborBrightness, "LabelIdx")
    
    trainingRecord.rdd.saveAsTextFile(output)
    
    // Thread.sleep(1000000000)
    
    // labelMatrix.show()
    // labelMatrix.rdd.saveAsTextFile(output)    
    
    // val imageMatrix = imageMatrixP.repartition(1)
    // val labelMatrix = labelMatrixP.repartition(1)
    
    // val mergedTable = labelMatrix
    // .join(imageMatrix, labelMatrix("NeighborIdx") === imageMatrix("ImageIdx"))
    // .select(Seq("LabelIdx","Label","Brightness").map(c => col(c)): _*)
    // .groupBy(labelMatrix("LabelIdx"))
    // .agg(collect_list("Brightness") as "Brightness")
    //.show()
    
    // mergedTable.show()
  }
}