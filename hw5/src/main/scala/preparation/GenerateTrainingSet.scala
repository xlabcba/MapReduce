package preparation

import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SaveMode
import scala.collection.parallel.immutable._

import fileLoader.LoadMultiStack

object GenerateTrainingSet {      
    
  // Structure for DataSet of Training Record
  case class TrainRecord(brightnesses: Array[Byte], label: Byte)

  // Writable structure for DataSet of Training Record
  case class TrainRecordOutput(brightnesses: List[Byte], label: Byte)

  // Given in-array index of a pixel and matrix size, 
  // return its matrix coordinates
  def toMatrixCoord(index: Int, imageSize: (Int, Int, Int)) : (Int, Int, Int) = {
    val (xDim, yDim, zDim) = imageSize;
    val z = index / (xDim * yDim)
    val remain = index - (z * xDim * yDim)
    val y = remain / xDim;
    val x = remain % xDim;
    (x, y, z)
  }
  
  // Given matrix coordinates, and matrix size,
  // return its in-array index
  def toArrayIdx(x: Int, y: Int, z: Int, imageSize: (Int, Int, Int)) : Int = {
    val (xDim, yDim, zDim) = imageSize;
    z * xDim * yDim + y * xDim + x
  }
  
  // Given distance, return its label
  def toLabel(value: Byte) : Byte = {
    if (value == 0 || value == 1) return 1
    else if (value > 3) return 0
    else return -1
  }
  
  // Given 1D array of Byte, and 3D matrix size, return its 3D matrix
  def toThreeDMatrix(array: Array[Byte], neighborSize: (Int, Int, Int)) : Array[Array[Array[Byte]]] = {
    val (xNum, yNum, zNum) = neighborSize
    val res = Array.ofDim[Byte](zNum, xNum, yNum)   
    for (i <- 0 until xNum * yNum * zNum) {
      val (z, y, x) = toMatrixCoord(i, (zNum, yNum, xNum))
      res(z)(x)(y) = array(i)
    }
    res
  }
  
  // Return true, if none of x, y, z has neighbor out of matrix boundaries
  def isValidIdx(x: Int, y: Int, z: Int, neighborSize: (Int, Int, Int), imageSize: (Int, Int, Int)) : Boolean = {
    val (xDim, yDim, zDim) = imageSize
    val (xNum, yNum, zNum) = neighborSize
    x > xNum / 2 && x < xDim - xNum / 2 && y > yNum / 2 && y < yDim - yNum / 2 && z >= zNum / 2 && z < zDim - zNum / 2
  }
  
  // Return true, if distance is valid
  def isValidDistance(d: Byte) : Boolean = {
    d <= 1 || d > 3
  }
  
  // Given array of neigbors, and neighbor matrix size
  // generate 8 combination of rotation and mirror
  def increaseDiversity(neighbor: Array[Byte], neighborSize: (Int, Int, Int)) : Array[Array[Byte]] = {
    var (xNum, yNum, zNum) = neighborSize
    val res = new Array[Array[Byte]](8)
    var idx = 0
    var prevRotate = neighbor
    // rotate 0 along z
    res(idx) = neighbor; idx += 1
    // rotate 0 along z + mirror along y
    res(idx) = mirror(prevRotate, (xNum, yNum, zNum)); idx += 1
    for (i <- 1 to 3) {
      // close-wise rotate 90 degree along z from previous status
      val currRotate = rotate(prevRotate, (xNum, yNum, zNum))
      res(idx) = currRotate; idx += 1
      // swap neighbor matrix size on x and y 
      val tmp = xNum; xNum = yNum; yNum = tmp;
      // mirror along y after 90 degree rotation
      res(idx) = mirror(currRotate, (xNum, yNum, zNum)); idx += 1
      prevRotate = currRotate
    }
    res.toArray 
  }
  
  // Given 1D array of neighbor and its matrix size
  // return 1D array of neighbor after rotate along z 90 degree and clock-wise
  def rotate(neighbors: Array[Byte], neighborSize: (Int, Int, Int)) : Array[Byte] = {
    val (xNum, yNum, zNum) = neighborSize
    val res = Array.ofDim[Byte](xNum * yNum * zNum)  
    for (i <- 0 until neighbors.length) {
      // for each neighbor, get its current matrix coordinates from index
      val (zCur, yCur, xCur) = toMatrixCoord(i, (zNum, yNum, xNum))
      // calculate new index by principle (newX,newY,newZ) = ((yNum-yCur-1),xCur,zCur)
      val newIdx = toArrayIdx(zCur, xCur, (yNum - yCur - 1), (zNum, xNum, yNum))
      res(newIdx) = neighbors(i)
    }
    res
  }
  
  // Given 1D array of neighbor and its matrix size
  // return 1D array of neighbor after mirror along y 
  def mirror(neighbors: Array[Byte], neighborSize: (Int, Int, Int)) : Array[Byte] = {
    val (xNum, yNum, zNum) = neighborSize
    val res = Array.ofDim[Byte](xNum * yNum * zNum)  
    for (i <- 0 until neighbors.length) {
      // for each neighbor, get its current matrix coordinates from index
      val (zCur, yCur, xCur) = toMatrixCoord(i, (zNum, yNum, xNum))
      // calculate new index by principle (newX,newY,newZ) = ((xNum-xCur-1),yCur,zCur)
      val newIdx = toArrayIdx(zCur, yCur, (xNum - xCur - 1), (zNum, yNum, xNum))
      res(newIdx) = neighbors(i)
    }
    res
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
    
    val sizes = new Array[(Int, Int, Int)](inputDists.length)
    var idx = 0
    for (dimensions <- args(2).split(delimitor)) {
      val array = dimensions.split(dimensionSplitor)
      val newElement = (Integer.parseInt(array(0)), Integer.parseInt(array(1)), Integer.parseInt(array(2)))
      sizes(idx) = newElement; idx += 1
    }
    
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
    val conf = new SparkConf()
    .setAppName("Image Preparation")
    val sc = new SparkContext(conf)
    
    val sqlContext= new SQLContext(sc)
    import sqlContext.implicits._
    
    // Load image files as Seq of Array[Array[Byte]] and zip with indices
    val imageSeq = (0 until inputImgs.length).flatMap(i => {
      val array = LoadMultiStack.loadImage(bucketName, inputImgs(i), sizes(i)._1, sizes(i)._2, sizes(i)._3)
      array.zipWithIndex.par.map{case(stackArray, stackIdx) => ((i.toByte, stackIdx.toByte), stackArray)}
    })
    
    // Broadcast as Map
    val imageMap = sc.broadcast(imageSeq.toMap)
        
    // Load distance files as Seq of Array[Array[Byte]] and zip with indices
    val distSeq = (0 until inputDists.length).flatMap(i => {
      val array = LoadMultiStack.loadImage(bucketName, inputDists(i), sizes(i)._1, sizes(i)._2, sizes(i)._3)
      array.zipWithIndex.par.map{case(stackArray, stackIdx) => (i.toByte, stackIdx.toByte, stackArray)}
    })
    
    // Turn to RDD
    val distRdd = sc.parallelize(distSeq)
    
    // Similar to map side join between each pixel on 2D stack of distance
    // and corresponding neighbor brightnesses in image
    val combinedRecords = distRdd
    .flatMap{case(imageIdx, stackIdx, stackArray) => {
      val (xDim, yDim, zDim) = sizes(imageIdx)
      val offset = stackIdx * xDim * yDim 
      // Each stack zip with in-plane index and filter out invalid records
      // then find neighbors for each pixel on the stack
      stackArray.zipWithIndex.par
      .filter{case(distance, i) => isValidIdx(i % xNum, i / xNum, stackIdx, neighborSize, sizes(imageIdx)) && isValidDistance(distance)}
      .map{case(distance, i) => {
        val resArray = new Array[Byte](xNum * yNum * zNum)
        val (x, y) = (i % xNum, i / xNum)
        var idx = 0
        for (dx <- (-xNum / 2) to (xNum / 2)) {
          for (dy <- (-yNum / 2) to (yNum / 2)) {
            for (dz <- (-zNum / 2) to (zNum / 2)) {
              // Get a neighbor from broadcast map
              resArray(idx) = imageMap.value((imageIdx, (stackIdx + dz).toByte))((y + dy) * xNum + (x + dx)); idx += 1
            }
          }
        }
        // Form training record 
        TrainRecord(resArray, toLabel(distance))
      }}.toList  
    }} 
    .toDS()
        
    // COUNT: 57174638 / 58262400
    // Sample with fraction then take random order to limit exact sample no.
    val sampledRecords = combinedRecords.sample(false, 0.05).orderBy(rand()).limit(sampleNo).persist()
        
    // For each neighbor list generate 8 combinations of rotation and mirror
    val trainingRecords = sampledRecords
    .flatMap{record => 
      increaseDiversity(record.brightnesses, neighborSize).par.map(n => TrainRecord(n, record.label)).toList}

    // Output based on the user-defined output mode
    if (mode.equalsIgnoreCase("tiff")) {
      val foreground = trainingRecords.filter { r => r.label == 1 }.first()
      LoadMultiStack.saveImages(toThreeDMatrix(foreground.brightnesses, neighborSize), bucketName, output + "/fore")
      // println("FOREGROUND: [IMAGE: " + foreground.imageIdx + "; COORD: " + toMatrixCoord(foreground.pixelIdx, sizes(foreground.imageIdx)))

      val background = trainingRecords.filter { t => t.label == 0 }.first()
      LoadMultiStack.saveImages(toThreeDMatrix(background.brightnesses, neighborSize), bucketName, output + "/back")
      // println("BACKGROUND: [IMAGE: " + background.imageIdx + "; COORD: " + toMatrixCoord(background.pixelIdx, sizes(background.imageIdx)))
    } else {
      trainingRecords
      .map(r => TrainRecordOutput(r.brightnesses.toList, r.label))
      .write.format("org.apache.spark.sql.json").save(output)
    }

    // Thread.sleep(1000000000)
  }
}