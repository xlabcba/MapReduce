package preparation

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SaveMode
import scala.collection.parallel.immutable._

import fileLoader.LoadMultiStack

object GenerateTrainingSet {      
    
  case class TrainRecord(imageIdx: Int, pixelIdx: Int, label: Int, brightnesses: Array[Byte])

  case class TrainRecordOutput(imageIdx: Int, pixelIdx: Int, label: Int, brightnesses: List[Byte])

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
  
  def findNeighbors(i: Int, neighborSize: (Int, Int, Int), imageSize: (Int, Int, Int)) : Array[Int] = {
    val (xNum, yNum, zNum) = neighborSize
    val (xDim, yDim, zDim) = imageSize
    val (x, y, z) = toMatrixCoord(i, imageSize)
    val res = ArrayBuffer.empty[Int]
    for (dx <- -(xNum / 2) to (xNum / 2)) {
      for (dy <- -(yNum / 2) to (yNum / 2)) {
        val (nx, ny) = (x + dx, y + dy)
        if (isValidPixel(nx, ny, 0, imageSize)) {
          res += toPlaneIdx(nx, ny, imageSize)
        }
      }
    }
    res.toArray
  }
  
  def isValidPixel(x: Int, y: Int, z: Int, imageSize: (Int, Int, Int)) : Boolean = {
    val (xDim, yDim, zDim) = imageSize
    x >= 0 && x < xDim && y >= 0 && y < yDim && z >= 0 && z < zDim
  }
  
  def isValidStack(stackIdx: Int, zNum: Int, zDim: Int) : Boolean = {
    stackIdx >= zNum / 2 && stackIdx < zDim - zNum / 2
  }
  
  def increaseDiversity(neighbor: Array[Byte], neighborSize: (Int, Int, Int)) : Array[Array[Byte]] = {
    var (xNum, yNum, zNum) = neighborSize
    val res = ArrayBuffer.empty[Array[Byte]]
    
    var prevRotate = toThreeDMatrix(neighbor, neighborSize)
    res += neighbor
    res += toOneDArray(mirror(prevRotate, (xNum, yNum, zNum)), (xNum, yNum, zNum)).toArray
    for (i <- 1 to 3) {
      val currRotate = rotate(prevRotate, (xNum, yNum, zNum))
      val tmp = xNum; xNum = yNum; yNum = tmp;
      res += toOneDArray(currRotate, (xNum, yNum, zNum)).toArray
      res += toOneDArray(mirror(currRotate, (xNum, yNum, zNum)), (xNum, yNum, zNum)).toArray
      prevRotate = currRotate
    }
    res.toArray 
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
  
  def toThreeDMatrix(array: Array[Byte], neighborSize: (Int, Int, Int)) : Array[Array[Array[Byte]]] = {
    val (xNum, yNum, zNum) = neighborSize
    val res = Array.ofDim[Byte](zNum, xNum, yNum)   
    for (i <- 0 until xNum * yNum * zNum) {
      val (z, y, x) = toMatrixCoord(i, (zNum, yNum, xNum))
      res(z)(x)(y) = array(i)
    }
    res
  }
  
  def toOneDArray(matrix: Array[Array[Array[Byte]]], neighborSize: (Int, Int, Int)) : Array[Byte] = {
    val (xNum, yNum, zNum) = neighborSize
    val res = Array.ofDim[Byte](xNum * yNum * zNum)  
    for (i <- 0 until xNum * yNum * zNum) {
      val (z, y, x) = toMatrixCoord(i, (zNum, yNum, xNum))
      res(i) = matrix(z)(x)(y)
    }
    res.toArray
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
    
    //Start the Spark context
//    val conf = new SparkConf()
//    .setAppName("Image Preparation")
//    val sc = new SparkContext(conf)
    val spark = SparkSession
    .builder()
    .appName("Image Preparation")
    .getOrCreate()
    val sc = spark.sparkContext
    
    import spark.implicits._
    
    val imageRddSeq = (0 until inputImgs.length).map(i => {
      val array = LoadMultiStack.loadImage(bucketName, inputImgs(i), sizes(i)._1, sizes(i)._2, sizes(i)._3)
      sc.parallelize(array).zipWithIndex.map{case(stackArray, stackIdx) => ((i, stackIdx.toInt), stackArray)}
    })
    
    val imageRddUnion = sc.union(imageRddSeq)
    
    val imageMap = sc.broadcast(imageRddUnion.collectAsMap)
        
    val distRddSeq = (0 until inputDists.length).map(i => {
      val array = LoadMultiStack.loadImage(bucketName, inputDists(i), sizes(i)._1, sizes(i)._2, sizes(i)._3)
      sc.parallelize(array).zipWithIndex.map{case(stackArray, stackIdx) => (i, stackIdx.toInt, stackArray)}
    })
    
    val distRddUnion = sc.union(distRddSeq)
    
    val r = scala.util.Random
    val combinedRecordsRDD = distRddUnion
    .filter{case(imageIdx, stackIdx, stackArray) => isValidStack(stackIdx, neighborSize._3, sizes(imageIdx)._3)}
    .flatMap{case(imageIdx, stackIdx, stackArray) => {
      val (xDim, yDim, zDim) = sizes(imageIdx)
      val offset = stackIdx * xDim * yDim 
      stackArray.zipWithIndex.par
      .map{case(distance, i) => {
        val resLst = ArrayBuffer.empty[Byte]
        val neighborLst = findNeighbors(i, neighborSize, sizes(imageIdx))
        if (neighborLst.length == xNum * yNum) {
          for (neighbor <- neighborLst) {
            for (dz <- -(zNum / 2) to (zNum / 2)) {
              val brightness = imageMap.value(imageIdx, stackIdx + dz)(neighbor)
              resLst += brightness
            }
          }
        }
        (imageIdx, i + offset, distance, resLst.toArray)
      }}
      .filter{case(imageIdx, stackIdx, distance, neighborLst) 
        => neighborLst.length == xNum * yNum * zNum}
      .toList
    }}
    .map{case(imageIdx, pixelIdx, distance, brightnessLst) => TrainRecord(imageIdx, pixelIdx, toLabel(distance), brightnessLst)}
    .filter{record => record.label != -1}
    .filter{record => r.nextInt(200) == 0}
    // .persist(StorageLevel.MEMORY_AND_DISK)
    
    val combinedRecords = spark.createDataset(combinedRecordsRDD)
    
    // COUNT: 57174638 / 58262400
    val sampleRecords = combinedRecords
    .orderBy(rand()).limit(sampleNo)
        
    val trainingRecords = sampleRecords
    .flatMap{record => 
      increaseDiversity(record.brightnesses, neighborSize).par.map(n => TrainRecord(record.imageIdx, record.pixelIdx, record.label, n)).toList}
    .persist(StorageLevel.MEMORY_AND_DISK)

    if (mode.equalsIgnoreCase("tiff")) {
      val foreground = trainingRecords.filter { r => r.label == 1 }.first()
      LoadMultiStack.saveImages(toThreeDMatrix(foreground.brightnesses, neighborSize), bucketName, output + "/fore")
      println("FOREGROUND: [IMAGE: " + foreground.imageIdx + "; COORD: " + toMatrixCoord(foreground.pixelIdx, sizes(foreground.imageIdx)))

      val background = trainingRecords.filter { t => t.label == 0 }.first()
      LoadMultiStack.saveImages(toThreeDMatrix(background.brightnesses, neighborSize), bucketName, output + "/back")
      println("BACKGROUND: [IMAGE: " + background.imageIdx + "; COORD: " + toMatrixCoord(background.pixelIdx, sizes(background.imageIdx)))
    } else {
      trainingRecords
      .map(r => TrainRecordOutput(r.imageIdx, r.pixelIdx, r.label, r.brightnesses.toList))
      .write.format("org.apache.spark.sql.json").mode(SaveMode.Append).save(output)
    }

    //Thread.sleep(1000000000)
  }
}