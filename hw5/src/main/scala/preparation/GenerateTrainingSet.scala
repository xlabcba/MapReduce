package preparation

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.udf
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
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
    
    // Constants
    val delimitor = "-"
    val dimensionSplitor = ","
    
    // Parse args
    val inputImages = args(0)
    val inputImgs = inputImages.split(delimitor)
    
    val inputDistances = args(1)
    val inputDists = inputDistances.split(delimitor)
    
    val sizesBuffer = ArrayBuffer.empty[(Int, Int, Int)]
    for (dimensions <- args(2).split(delimitor)) {
      val array = dimensions.split(dimensionSplitor)
      val newElement = (Integer.parseInt(array(0)), Integer.parseInt(array(1)), Integer.parseInt(array(2)))
      sizesBuffer += newElement
    }
    val sizes = sizesBuffer.toArray
    
    var output = args(3)
    val sampleNo = Integer.parseInt(args(4))
    
    val neighborNo = args(5).split(delimitor)
    val xNum = Integer.parseInt(neighborNo(0))
    val yNum = Integer.parseInt(neighborNo(1))
    val zNum = Integer.parseInt(neighborNo(2))
    val neighborSize = (xNum, yNum, zNum)
    
    val mode = args(6)
    var bucketName = ""
    if (args.length > 7) {
      bucketName = args(7) 
      if (mode.toLowerCase().contains("text")) {
        output = "s3://" + bucketName + "/" + output
      }
    }
    
    // val inputImgs = Array("input/1_image.tiff")
    // val inputDists = Array("input/1_dist.tiff")
    // val inputImgs = Array("input/1_image.tiff", "input/2_image.tiff", "input/3_image.tiff", "input/4_image.tiff", "input/6_image.tiff")
    // val inputDists = Array("input/1_dist.tiff", "input/2_dist.tiff", "input/3_dist.tiff", "input/4_dist.tiff", "input/6_dist.tiff")
    // val sizes = Array((512, 512, 60), (512, 512, 33), (512, 512, 44), (512, 512, 51), (512, 512, 46))
    // val (xNum, yNum, zNum) = neighborSize
    // val sampleNo = 12500
    // val partitionNo = 4
    // val bucketName = "lixiebucket"
    
    //Start the Spark context
    val conf = new SparkConf()
    .setAppName("Image Preparation")
    val sc = new SparkContext(conf)
    
    val imageRDD = (0 until inputImgs.length).map(i => {
      val array = LoadMultiStack.loadImage(bucketName, inputImgs(i), sizes(i)._1, sizes(i)._2, sizes(i)._3)
      sc.parallelize(array).zipWithIndex.map{case(stackArray, stackIdx) => ((i, stackIdx.toInt), stackArray)}
    })
    .reduce(_ union _)
    
    val imageMap = sc.broadcast(imageRDD.collectAsMap)
        
    val r = scala.util.Random
    val combinedRecords = (0 until inputDists.length).map(i => {
      val array = LoadMultiStack.loadImage(bucketName, inputDists(i), sizes(i)._1, sizes(i)._2, sizes(i)._3)
      sc.parallelize(array).zipWithIndex.map{case(stackArray, stackIdx) => (i, stackIdx.toInt, stackArray)}
    })
    .reduce(_ union _)
    .filter{case(imageIdx, stackIdx, stackArray) => isValidStack(stackIdx, neighborSize._3, sizes(imageIdx)._3)}
    .flatMap{case(imageIdx, stackIdx, stackArray) => {
      val (xDim, yDim, zDim) = sizes(imageIdx)
      val offset = stackIdx * xDim * yDim 
      stackArray.zipWithIndex.par
      .map{case(distance, i) => {
        val resLst = ListBuffer.empty[Byte]
        val neighborLst = findNeighbors(i, neighborSize, sizes(imageIdx))
        if (neighborLst.length == xNum * yNum) {
          for (neighbor <- neighborLst) {
            for (dz <- -(zNum / 2) to (zNum / 2)) {
              val brightness = imageMap.value(imageIdx, stackIdx + dz)(neighbor)
              resLst += brightness
            }
          }
        }
        (imageIdx, i + offset, distance, resLst.toList)
      }}
      .filter{case(imageIdx, stackIdx, distance, neighborLst) 
        => neighborLst.length == xNum * yNum * zNum}
      .toList
    }}
    .map{case(imageIdx, pixelIdx, distance, brightnessLst) => (imageIdx, pixelIdx, toLabel(distance), brightnessLst)}
    .filter{case(imageIdx, pixelIdx, label, brightnessLst) => label != -1}
    .filter{record => r.nextInt(200) == 0}
    .persist(StorageLevel.MEMORY_AND_DISK)
    
    // COUNT: 57174638 / 58262400
//    val sampleRecords = combinedRecords.takeSample(false, sampleNo)
//        
//    val trainingRecords = sc.parallelize(sampleRecords)
//    .flatMap{case(imageIdx, pixelIdx, label, brightnessLst) => 
//      increaseDiversity(brightnessLst, neighborSize).par.map((imageIdx, pixelIdx, label, _)).toList}
//    .persist(StorageLevel.MEMORY_AND_DISK)
    
    val foreground = combinedRecords.filter{t => t._3 == 1}.first()
    LoadMultiStack.saveImages(toThreeDMatrix(foreground._4, neighborSize), bucketName, output + "/fore")
    println("FOREGROUND: [IMAGE: " + foreground._1 + "; COORD: " + toMatrixCoord(foreground._2, sizes(foreground._1)) + "; LABEL:" + foreground._3 + "; NEIGHBORS: " + foreground._4)
    
    val background = combinedRecords.filter{t => t._3 == 0}.first()
    LoadMultiStack.saveImages(toThreeDMatrix(background._4, neighborSize), bucketName, output + "/back")
    println("BACKGROUND: [IMAGE: " + background._1 + "; COORD: " + toMatrixCoord(background._2, sizes(background._1)) + "; LABEL:" + background._3 + "; NEIGHBORS: " + background._4)

    //Thread.sleep(1000000000)
  }
}