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
import org.apache.spark.sql.Row
import scala.collection.parallel.immutable._

import fileLoader.LoadMultiStack

object GenerateTrainingSet {      
  
  def toMatrixCoord(index: Int, imageSize: (Int, Int, Int)) : (Int, Int, Int) = {
    val (xDim, yDim, zDim) = imageSize;
    val z = index / (xDim * yDim)
    val remain = index - (z * xDim * yDim)
    val y = remain / xDim;
    val x = remain % xDim;
    (x, y, z)
  }
  
  def toArrayIdx(x: Int, y: Int, z: Int, imageSize: (Int, Int, Int)) : Int = {
    val (xDim, yDim, zDim) = imageSize;
    z * xDim * yDim + y * xDim + x
  }
  
  def toPlaneIdx(x: Int, y: Int, imageSize: (Int, Int, Int)) : Int = {
    val (xDim, yDim, zDim) = imageSize;
    y * xDim + x
  }
  
  def toLabel(value: Byte) : Int = {
    if (value == 0 || value == 1) return 1
    else if (value > 3) return 0
    else return -1
  }
  
  def findNeighbors(i: Int, neighborSize: (Int, Int, Int), imageSize: (Int, Int, Int)) : List[Int] = {
    val (xNum, yNum, zNum) = neighborSize
    val (xDim, yDim, zDim) = imageSize
    val (x, y, z) = toMatrixCoord(i, imageSize)
    val res = ListBuffer.empty[Int]
    for (dx <- -(xNum / 2) to (xNum / 2)) {
      for (dy <- -(yNum / 2) to (yNum / 2)) {
        val (nx, ny) = (x + dx, y + dy)
        if (isValidPixel(nx, ny, 0, imageSize)) {
          res += toPlaneIdx(nx, ny, imageSize)
        }
      }
    }
    res.toList
  }
  
  def isValidPixel(x: Int, y: Int, z: Int, imageSize: (Int, Int, Int)) : Boolean = {
    val (xDim, yDim, zDim) = imageSize
    x >= 0 && x < xDim && y >= 0 && y < yDim && z >= 0 && z < zDim
  }
  
  def isValidStack(stackIdx: Int, zNum: Int, zDim: Int) : Boolean = {
    stackIdx >= zNum / 2 && stackIdx < zDim - zNum / 2
  }
  
  def increaseDiversity(neighbor: List[Byte], neighborSize: (Int, Int, Int)) : List[List[Byte]] = {
    var (xNum, yNum, zNum) = neighborSize
    val res = ListBuffer.empty[List[Byte]]
    
    var prevRotate = toThreeDMatrix(neighbor, neighborSize)
    res += neighbor
    res += toOneDArray(mirror(prevRotate, (xNum, yNum, zNum)), (xNum, yNum, zNum)).toList
    for (i <- 1 to 3) {
      val currRotate = rotate(prevRotate, (xNum, yNum, zNum))
      val tmp = xNum; xNum = yNum; yNum = tmp;
      res += toOneDArray(currRotate, (xNum, yNum, zNum)).toList
      res += toOneDArray(mirror(currRotate, (xNum, yNum, zNum)), (xNum, yNum, zNum)).toList
      prevRotate = currRotate
    }
    res.toList 
  }
  
  def rotate(matrix: Array[Array[Array[Byte]]], neighborSize: (Int, Int, Int)) : Array[Array[Array[Byte]]] = {
    val (xNum, yNum, zNum) = neighborSize
    val res = Array.ofDim[Byte](zNum, yNum, xNum)   
    for (z <- 0 until zNum) {
      res(z) = matrix(z).transpose.reverse
      // println("-----------------------------------------")
      // matrix(z) foreach { row => row foreach print; println }
      // res(z) foreach { row => row foreach print; println }
      // println("-----------------------------------------")
    }
    res
  }
  
  def mirror(matrix: Array[Array[Array[Byte]]], neighborSize: (Int, Int, Int)) : Array[Array[Array[Byte]]] = {
    val (xNum, yNum, zNum) = neighborSize
    val res = Array.ofDim[Byte](zNum, xNum, yNum)   
    for (z <- 0 until zNum) {
      res(z) = matrix(z).reverse
      // println("-----------------------------------------")
      // matrix(z) foreach { row => row foreach print; println }
      // res(z) foreach { row => row foreach print; println }
      // println("-----------------------------------------")
    }
    res
  }
  
  def toThreeDMatrix(array: List[Byte], neighborSize: (Int, Int, Int)) : Array[Array[Array[Byte]]] = {
    val (xNum, yNum, zNum) = neighborSize
    val res = Array.ofDim[Byte](zNum, xNum, yNum)   
    for (i <- 0 until xNum * yNum * zNum) {
      val (z, y, x) = toMatrixCoord(i, (zNum, yNum, xNum))
      res(z)(x)(y) = array(i)
    }
    res
  }
  
  def toOneDArray(matrix: Array[Array[Array[Byte]]], neighborSize: (Int, Int, Int)) : List[Byte] = {
    val (xNum, yNum, zNum) = neighborSize
    val res = Array.ofDim[Byte](xNum * yNum * zNum)  
    for (i <- 0 until xNum * yNum * zNum) {
      val (z, y, x) = toMatrixCoord(i, (zNum, yNum, xNum))
      res(i) = matrix(z)(x)(y)
    }
    res.toList
  }
        
  def main(args: Array[String]) = {
    
    // Parse args
    // val input = args(0)
    // val output = args(1)
    val inputImages = Array("input/1_image.tiff")
    val inputDists = Array("input/1_dist.tiff")
    //val inputImages = Array("input/1_image.tiff", "input/2_image.tiff", "input/3_image.tiff", "input/4_image.tiff", "input/6_image.tiff")
    //val inputDists = Array("input/1_dist.tiff", "input/2_dist.tiff", "input/3_dist.tiff", "input/4_dist.tiff", "input/6_dist.tiff")
    val sizes = Array((512, 512, 60), (512, 512, 33), (512, 512, 44), (512, 512, 51), (512, 512, 46))
    val neighborSize = (9, 9, 1)
    val sampleNo = 12500
    val partitionNo = 4
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
    
    val imageRDD = (0 until inputImages.length).map(i => {
      val array = LoadMultiStack.loadImage(inputImages(i), sizes(i)._1, sizes(i)._2, sizes(i)._3)
      sc.parallelize(array).zipWithIndex.map{case(stackArray, stackIdx) => ((i, stackIdx.toInt), stackArray)}
    })
    .reduce(_ union _)
    
    val imageMap = sc.broadcast(imageRDD.collectAsMap)
        
    val r = scala.util.Random
    val combinedRecords = (0 until inputDists.length).map(i => {
      val array = LoadMultiStack.loadImage(inputDists(i), sizes(i)._1, sizes(i)._2, sizes(i)._3)
      sc.parallelize(array, partitionNo).zipWithIndex.map{case(stackArray, stackIdx) => ((i, stackIdx.toInt), stackArray)}
    })
    .reduce(_ union _)
    .filter{case((imageIdx, stackIdx), stackArray) => isValidStack(stackIdx, neighborSize._3, sizes(imageIdx)._3)}
    .flatMap{case((imageIdx, stackIdx), stackArray) => {
      val (xNum, yNum, zNum) = neighborSize
      val (xDim, yDim, zDim) = sizes(imageIdx)
      val res = ListBuffer.empty[(Int, Int, Byte, List[Byte])]
      val offset = stackIdx * xDim * yDim 
      for (i <- 0 until yDim * xDim) {
        val pixel = ListBuffer.empty[Byte]
        val neighborLst = findNeighbors(i, neighborSize, sizes(imageIdx))
        if (neighborLst.length == xNum * yNum) {
          for (neighbor <- neighborLst) {
            for (dz <- -(zNum / 2) to (zNum / 2)) {
              val brightness = imageMap.value(imageIdx, stackIdx + dz)(neighbor)
              pixel += brightness
            }
          }
          val curRes = (imageIdx, i + offset, stackArray(i), pixel.toList)
          res += curRes
        }
      }
      res.toList
    }}
    .map{case(imageIdx, pixelIdx, distance, brightnessLst) => (imageIdx, pixelIdx, toLabel(distance), brightnessLst)}
    .filter{case(imageIdx, pixelIdx, label, brightnessLst) => label != -1}
    .filter{record => r.nextInt(200) == 0}
    .toDS()
    
    // COUNT: 57174638 / 58262400
    // val sampleRecords = combinedRecords.orderBy(rand()).limit(sampleNo)
    //.rdd
    //.map{case Row(imageIdx: Int, pixelIdx: Int, label: Int, brightnessLst: List[Byte]) => (imageIdx, pixelIdx, label, brightnessLst)}
    // .map(row => (row(1), row(2), row(3), row.getAs[List[Byte]](4)))
    // val sampleRecords = sc.parallelize(combinedRecords.takeSample(false, sampleNo)).persist()
        
//    val trainingRecords = sampleRecords.flatMap{case(imageIdx, pixelIdx, label, brightnessLst) => 
//      increaseDiversity(brightnessLst, neighborSize).par.map((imageIdx, pixelIdx, label, _)).toList}
    
    val foreground = combinedRecords.filter{t => t._3 == 1}.first()
    val imageIndex = foreground._1
    val (xIndex, yIndex, zIndex) = toMatrixCoord(foreground._2, sizes(imageIndex))
    println("image: " + imageIndex + "; x: " + xIndex + "; y" + yIndex + "; z: " + zIndex)
    LoadMultiStack.saveImages(toThreeDMatrix(foreground._4, neighborSize), "fore")
    
    // val background = combinedRecords.filter{t => t._3 == 0}.first()._4
    // LoadMultiStack.saveImages(toThreeDMatrix(background, neighborSize), "back")

    //Thread.sleep(1000000000)
  }
}