package temperatureAnalysis

import org.apache.spark.SparkConf 
import org.apache.spark.SparkContext 
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions 
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner


object TemperatureAnalysis{
	
	case class StationRecord(stationId:String,year:Int,rdType:String,reading:Double)
	
	case class StationAccumu(minSum:Double,minCnt:Int,maxSum:Double,maxCnt:Int)
	
    case class CompositeKey(stationId: String, year: Int)
       
	/**
	 * Defines partition behavior of compositeKey
	 * as partition by stationId
	 *  
	 * @param Int
	 * @return
	 */
    class FirstKeyPartitioner(partitions: Int) extends Partitioner {
        val delegate = new HashPartitioner(partitions)
        override def numPartitions = delegate.numPartitions
        override def getPartition(key: Any): Int = {
          val k = key.asInstanceOf[CompositeKey]
          delegate.getPartition(k.stationId)
        }
    }
	
	/**
	 * Defines sorting order of composite key:
	 * Firstly sort by stationId in increasing order
	 * For same stationId, sort by year in increasing order
	 */
	object CompositeKey {    
		implicit def orderingByStationIdAndYear[A <: CompositeKey] : Ordering[A] = {
				Ordering.by(ck => (ck.stationId, ck.year))
		}
	}
	
	/**
	 * This method maps one record line into one StationRecord
	 *  
	 * @param String
	 * @return StationRecord
	 */
	def parseLineToStationRecord(line: String) : StationRecord = {
		val parts = line.split(",", -1)
		StationRecord(parts(0), parts(1).substring(0,4).toInt, parts(2), parts(3).toDouble)
	}

	/**
	 * This method takes one stationRecord with (stationId, year, readingType, readingNumber)
	 * and a stationAccumu with (maxSum, maxCount, minSum, minCount)
	 * and merges readingNumber into corresp. fields based on readingType
	 *  
	 * @param StationRecord
	 * @return StationAccumu
	 */
	def mergeRecordToAccumu(record: StationRecord, accumu: StationAccumu) : StationAccumu = {
		val minSum = if (record.rdType == "TMIN") accumu.minSum + record.reading else accumu.minSum
		val minCnt = if (record.rdType == "TMIN") accumu.minCnt + 1 else accumu.minCnt
		val maxSum = if (record.rdType == "TMAX") accumu.maxSum + record.reading else accumu.maxSum
		val maxCnt = if (record.rdType == "TMAX") accumu.maxCnt + 1 else accumu.maxCnt
		StationAccumu(minSum, minCnt, maxSum, maxCnt)
	}

	/**
	 * This method takes one stationRecord with (stationId, year, readingType, readingNumber)
	 * and create corresponding stationAccumu with (maxSum, maxCount, minSum, minCount)
	 * and return (K,V) as pair of (stationId, stationAccumu)
	 *  
	 * @param StationRecord
	 * @return Tuple(String, StationAccumu)
	 */
	def mapRecordToAccumuTuple(record: StationRecord) : (String, StationAccumu) = {
		(record.stationId, mapRecordToAccumu(record))
	}
	
	/**
	 * This method takes one stationRecord with (stationId, year, readingType, readingNumber)
	 * and create corresponding stationAccumu with (maxSum, maxCount, minSum, minCount)
	 *  
	 * @param StationRecord
	 * @return StationAccumu
	 */
	def mapRecordToAccumu(record: StationRecord) : StationAccumu = {
			val minSum = if (record.rdType == "TMIN")  record.reading else 0.0
			val minCnt = if (record.rdType == "TMIN") 1 else 0
			val maxSum = if (record.rdType == "TMAX") record.reading else 0.0
			val maxCnt = if (record.rdType == "TMAX") 1 else 0
			StationAccumu(minSum, minCnt, maxSum, maxCnt)
	}

	/**
	 * This method takes one stationAccumu(s) with (maxSum, maxCount, minSum, minCount)
	 * and a tuple(String, Double) as pair of (readingType, readingNumber)
	 * and merges readingNumber into max fields or min fields based on the readingType
	 *  
	 * @param StationAccumu
	 * @param Tuple(String, Double)
	 * @return StationAccumu
	 */
	def accumuRecord(accumu: StationAccumu, record: (String, Double)) : StationAccumu = {
		val minSum = if (record._1 == "TMIN") accumu.minSum + record._2 else accumu.minSum
		val minCnt = if (record._1 == "TMIN") accumu.minCnt + 1 else accumu.minCnt
		val maxSum = if (record._1 == "TMAX") accumu.maxSum + record._2 else accumu.maxSum
		val maxCnt = if (record._1 == "TMAX") accumu.maxCnt + 1 else accumu.maxCnt
		StationAccumu(minSum, minCnt, maxSum, maxCnt)
	}

	/**
	 * This method takes two stationAccumu(s) with (maxSum, maxCount, minSum, minCount)
	 * and merge all corresponding fields together into one stationAccumu
	 *  
	 * @param StationAccumu
	 * @param StationAccumu
	 * @return StationAccumu
	 */
	def mergeAccumu(part1: StationAccumu, part2: StationAccumu) : StationAccumu = {
		StationAccumu(part1.minSum + part2.minSum, part1.minCnt + part2.minCnt, part1.maxSum + part2.maxSum, part1.maxCnt + part2.maxCnt)
	}
	
	/**
	 * This method takes stationRecord with (stationId, year, readingType, readingNumber)
	 * and create (K, V) tuple as ((stationId, year), (readingType, readingNumber))
	 *  
	 * @param StationRecord
	 * @return Tuple(CompositeKey, StationAccumu)
	 */
	def createCompositeKeyValueTuple(record: StationRecord) : (CompositeKey,StationAccumu) = {
		val ck = new CompositeKey(record.stationId, record.year)
		(ck,mapRecordToAccumu(record))
	}
	
	/**
	 * This method is similar to reduce call in M-R secondary sort reducer
	 * It takes sorted stationRecords with same stationId and increasing year
	 * It accumulate records (max/minSum & max/minCount) for same stationId and same year
	 * Then calculate averages (maxAvg & minAvg) and append to result list
	 *  
	 * @param Iterator[CompositeKey]
	 * @return Iterator[(String, List[(Int, Double, Double)])]
	 */
	def groupResult(records: Iterator[(CompositeKey, StationAccumu)]) : Iterator[(String, List[(Int, Double, Double)])] = {
			val hashmap = new HashMap[String, List[(Int, Double, Double)]]
			var currStationId = "fakeStation"
			var currYear = 0
			var currAccumu = new StationAccumu(0.0, 0, 0.0, 0)
			records.foreach{record => {
				if (record._1.stationId == currStationId) {
					if (record._1.year == currYear) {
						// If same stationId and year,
						// accumulate record to current accumu record
						currAccumu = mergeAccumu(currAccumu, record._2)
					} else {
						// If same stationId but different year, 
						// calculate and put averages of previous accumu record 
						if (currStationId != "fakeStation" && currYear != 0) {
							val yearAverages = (currYear, currAccumu.minSum / currAccumu.minCnt, currAccumu.maxSum / currAccumu.maxCnt)
							if (hashmap contains currStationId) {
								hashmap(currStationId) = hashmap(currStationId) :+ yearAverages
							} else {
								hashmap(currStationId) = List(yearAverages)
							}
						}
						// reset accumu record as current record 
						currYear = record._1.year
						currAccumu = record._2
					}
				} else {
					// If different stationId,
					// calculate and put averages of previous accumu record 
					if (currStationId != "fakeStation" && currYear != 0) {
						val yearAverages = (currYear, currAccumu.minSum / currAccumu.minCnt, currAccumu.maxSum / currAccumu.maxCnt)
						if (hashmap contains currStationId) {
							hashmap(currStationId) = hashmap(currStationId) :+ yearAverages
						} else {
							hashmap(currStationId) = List(yearAverages)
						}
					}
					// reset accumu record as current record 
					currStationId = record._1.stationId
					currYear = record._1.year
					currAccumu = record._2
					
				}
			}}
			// process rest of data
			if (currStationId != "fakeStation" && currYear != 0) {
				// calculate and put averages of previous accumu record 
				val yearAverages = (currYear, currAccumu.minSum / currAccumu.minCnt, currAccumu.maxSum / currAccumu.maxCnt)
				if (hashmap contains currStationId) {
					hashmap(currStationId) = hashmap(currStationId) :+ yearAverages
				} else {
					hashmap(currStationId) = List(yearAverages)
				}
			}
			hashmap.toIterator
	}

	def main(args: Array[String]) = {
		
		// Setup running constants
		// NoCombiner, Combiner, InMapperComb Constants
	    val inputs = "input/1991.csv"
	    val mode = "nocombiner"
	    // SecondarySort Constants
		// val inputs = "input/1880.csv,input/1881.csv,input/1882.csv,input/1883.csv,input/1884.csv,input/1885.csv,input/1886.csv,input/1887.csv,input/1888.csv,input/1889.csv"
		// val mode = "secondarysort"

		//Start the Spark context
		val conf = new SparkConf()
			.setAppName("Temperature Analysis")
			.setMaster("local")
		val sc = new SparkContext(conf)

		// Load csv file as RDD
		val lines = sc.textFile(inputs)
    
		// Map line to record and filter TMAX & TMIN
		val allRecords = lines
			.map(parseLineToStationRecord)
			.filter{ record => record.rdType == "TMAX" || record.rdType == "TMIN" }

		mode match {
			// If NoCombiner mode,
			// In each partition (as mapper), map RDD to pairRDD as (key stationId, value (readingType, readingNumber)
			// group by same key stationId (mock shuffle & sort)
			// for each same stationId group, accumulate record and calculate averages (MAX_AVG & MIN_AVG)
			case "nocombiner" => allRecords
				.mapPartitions{records => records.map(record => (record.stationId, (record.rdType, record.reading)))}
		  		.groupByKey()
		  		.mapValues(record => record.aggregate(StationAccumu(0.0, 0, 0.0, 0))(accumuRecord, mergeAccumu))
		  		.mapValues(record => (record.minSum / record.minCnt, record.maxSum / record.maxCnt))
		  		.saveAsTextFile("output/")	
		  	// If Combiner mode,
		  	// In each partition (as mapper), map RDD to pairRDD as (key stationId, value (maxSum, maxCnt, minSum, minCnt))
		  	// do combiner task and accumulation part of reducer task together by reduceByKey
		  	// for each accumulated stationRecord, calculate averages (MAX_AVG & MIN_AVG)
			case "combiner" => allRecords
				.mapPartitions{ records => records.map(mapRecordToAccumuTuple) }
				.reduceByKey(mergeAccumu)
				.mapValues(record => (record.minSum / record.minCnt, record.maxSum / record.maxCnt)) 
				.saveAsTextFile("output/")
			// If InMapperComb mode,
			// In each partition (as mapper),
			//    init hashmap(key stationId, value (maxSum, maxCnt, minSum, minCnt))
			//    for each RDD record, map to pairRDD and accumulate to corresp. hashmap entry
			// group by same key stationId (mock shuffle & sort)
			// for each same stationId group, accumulate record and calculate averages (MAX_AVG & MIN_AVG)
			case "inmappercomb" => allRecords
				.mapPartitions(records => {
					val hashmap = new HashMap[String, StationAccumu] 
					records.foreach{record => hashmap += (record.stationId -> mergeRecordToAccumu(record, hashmap.getOrElse(record.stationId, StationAccumu(0.0, 0, 0.0, 0))))}
					hashmap.toIterator
				})
				.groupByKey()
				.mapValues(record => record.aggregate(StationAccumu(0.0, 0, 0.0, 0))(mergeAccumu, mergeAccumu))
				.mapValues(record => (record.minSum / record.minCnt, record.maxSum / record.maxCnt))
				.saveAsTextFile("output/")
			// In SecondarySort mode,
			// map RDD to pairRDD as (key (stationId, year), value (readingType, readingNumber))
			// partition by stationId, and sort firstly by stationId and then by year both in increasing order
			// for each record with same stationId, group them by firstly accumulating and then calculating averages
			case "secondarysort" => allRecords
				.map(record => createCompositeKeyValueTuple(record))
				.repartitionAndSortWithinPartitions(new FirstKeyPartitioner(allRecords.partitions.size))
				.mapPartitions{groupResult(_)}
	  			.saveAsTextFile("output/")	
		}
		
		//Stop the Spark context  
		sc.stop
	}
		  		
}

