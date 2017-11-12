package preparation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.udf
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SaveMode

import fileLoader.LoadMultiStack

object GenerateTrainingSet {
  
  def toMatrixCoord(index: Int) : (Int, Int, Int) = {
    val xDim = 512; val yDim = 512; val zDim = 60;
    val z = index / (xDim * yDim)
    val remain = index - (z * xDim * yDim)
    val y = remain / xDim;
    val x = remain % xDim;
    (x, y, z)
  }
  
  def toArrayIdx(x: Int, y: Int, z: Int) : Int = {
    val xDim = 512; val yDim = 512; val zDim = 60;
    z * xDim * yDim + y * xDim + x
  }
  
  def toPlaneIdx(x: Int, y: Int) : Int = {
    val xDim = 512; val yDim = 512
    y * xDim + x
  }
  
  def toLabel(value: Byte) : Int = {
    if (value == 0 || value == 1) {
      return 1
    } else if (value > 3) {
      return 0
    } else {
      return -1
    }
  }
  
  def generateNeighbors(i: Int, x: Int, y: Int, z: Int) : List[(Int, Int)] = {
    val xNum = 3; val yNum = 3; val zNum = 3;
    val xDim = 512; val yDim = 512; val zDim = 60;
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
  
  def emitNeighbors(index: Long, value: Byte) : List[(Int, List[Int])] = {
    val xNum = 3; val yNum = 3; val zNum = 3;
    val xDim = 512; val yDim = 512; val zDim = 60;
    val (x, y, z) = toMatrixCoord(index.toInt)
    val res = new Array[(Int, List[Int])](xNum * yNum * zNum)
    var cur = 0
    for (dx <- -(xNum / 2) to (xNum / 2)) {
      for (dy <- -(yNum / 2) to (yNum / 2)) {
        for (dz <- -(zNum / 2) to (zNum / 2)) {
          val (nx, ny, nz) = (x + dx, y + dy, z + dz)
          var neighborIdx = 0
          if (nx < 0 || nx >= xDim || ny < 0 || ny >= yDim || nz < 0 || nz >= zDim) {
            neighborIdx = -1
          } else {
            neighborIdx = nz * xNum * yNum + ny * xNum + nx
          }
          // val currIdx = (zNum / 2 + dz) * xNum * yNum + (yNum / 2 + dy) * xNum + (xNum / 2 + dx)
          res(cur) = (neighborIdx, List(value.toInt))
          cur += 1
        }
      }
    }
    return res.toList
  }
  
  def emitNeighborPlanes(z: Long, stackMatrix: Array[Byte]) : List[(Int, List[(Int, Array[Byte])])] = {
    val (xNum, yNum, zNum) = (3, 3, 3)
    val (xDim, yDim, zDim) = (512, 512, 60)
    val curz = z.toInt
    val res = ListBuffer.empty[(Int, List[(Int, Array[Byte])])]
    for (dz <- -(zNum / 2) to (zNum / 2)) {
      val nz = curz + dz
      if (0 <= nz && nz < zDim) {
        val add = (nz, List((curz, stackMatrix)))
        res += add
      }
    }
    res.toList
  }
  
  def emitNeighborPixels(z: Int, stackMap: Map[Int, Array[Byte]]) : List[(Int, List[Byte])] = {
    val (xNum, yNum, zNum) = (3, 3, 3)
    val (xDim, yDim, zDim) = (512, 512, 60)
    val res = ListBuffer.empty[(Int, List[Byte])]
    val offset = z * xDim * yDim 
    for (i <- 0 until yDim * xDim) {
      val pixel = ListBuffer.empty[Byte]
      val neighborLst = findNeighbors(i)
      if (neighborLst.length == xNum * yNum) {
        for (dz <- -(zNum / 2) to (zNum / 2)) {
          for (neighbor <- neighborLst) {
            val brightness = stackMap(z + dz)(neighbor)
            pixel += brightness
          }
        }
        val curRes = (i + offset, pixel.toList)
        res += curRes
      }
    }
    res.toList
  }
  
  def findNeighbors(i: Int) : List[Int] = {
    val (xNum, yNum, zNum) = (3, 3, 3)
    val (xDim, yDim, zDim) = (512, 512, 60)
    val (x, y, z) = toMatrixCoord(i)
    val res = ListBuffer.empty[Int]
    for (dx <- -(xNum / 2) to (xNum / 2)) {
      for (dy <- -(yNum / 2) to (yNum / 2)) {
        val (nx, ny) = (x + dx, y + dy)
        if (isValidPixel(nx, ny, 0)) {
          res += toPlaneIdx(nx, ny)
        }
      }
    }
    res.toList
  }
  
  def isValidPixel(x: Int, y: Int, z: Int) : Boolean = {
    val (xDim, yDim, zDim) = (512, 512, 60)
    x >= 0 && x < xDim && y >= 0 && y < yDim && z >= 0 && z < zDim
  }
  
  def isOnEdge(i: Int) : Boolean = {
    val (xDim, yDim, zDim) = (512, 512, 60)
    val (x, y, z) = toMatrixCoord(i)
    x == 0 || x == (xDim - 1) || y == 0 || y == (yDim - 1) || z == 0 || z == (zDim - 1)
  }
      
  def main(args: Array[String]) = {
    
    // Parse args
    // val input = args(0)
    // val output = args(1)
    val inputImage = "input/1_image.tiff"
    val inputDist = "input/1_dist.tiff"
    val output = "output"
    
    //Start the Spark context
    val conf = new SparkConf()
    .setAppName("Image Preparation")
    //.set("spark.network.timeout", "6000s")
    .set("spark.executor.memory", "8g")
    //.set("spark.executor.heartbeatInterval", "60s")
    val sc = new SparkContext(conf)
    
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val imageArray = LoadMultiStack.loadImage(inputImage, 512, 512, 60)
    val imageDF = sc.parallelize(imageArray).zipWithIndex
    .flatMap{case(stackMatrix, z) => emitNeighborPlanes(z, stackMatrix)}
    .reduceByKey(_ ::: _)
    .filter{case(z, stackLst) => stackLst.length == 3}
    .mapValues(_.toMap)
    .flatMap{case(z, stackMap) => emitNeighborPixels(z, stackMap)}
    .toDF("index", "brightness")
    // .persist(StorageLevel.MEMORY_AND_DISK)
    
    // imageDF.show()
    
    val distArray = LoadMultiStack.loadDist(inputDist, 512, 512, 60)
    val labelDF = sc.parallelize(distArray).zipWithIndex
    .map{case(distance, index) => (index.toInt, toLabel(distance))}
    .filter{case(index, label) => label != -1 && !isOnEdge(index)}
    .toDF("index", "label")
    // .persist(StorageLevel.MEMORY_AND_DISK)
    
    // labelDF.show()
    
    val trainingSet = imageDF.join(broadcast(labelDF), "index")
    
    trainingSet.show()
    //trainingSet.write.format("org.apache.spark.sql.json").mode(SaveMode.Append).save(output)
    //trainingSet.write.json(output)
//    
    Thread.sleep(1000000000)
  }
}