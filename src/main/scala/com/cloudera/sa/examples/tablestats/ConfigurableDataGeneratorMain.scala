package com.cloudera.sa.examples.tablestats

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{StringType, LongType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.util.Random


/**
 * Created by ted.malaska on 6/29/15.
 */
object ConfigurableDataGeneratorMain {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("ConfigurableDataGeneratorMain <outputPath> <numberOfColumns> <numberOfRecords> <numberOfPartitions> <local>")
      return
    }

    val outputPath = args(0)
    val numberOfColumns = args(1).toInt
    val numberOfRecords = args(2).toInt
    val numberOfPartitions = args(3).toInt
    val runLocal = (args.length == 5 && args(4).equals("L"))

    var sc: SparkContext = null
    if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      sc = new SparkContext("local", "test", sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName("ConfigurableDataGeneratorMain")
      sc = new SparkContext(sparkConfig)
    }

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //Part A
    val rowRDD = sc.parallelize( (0 until numberOfPartitions).map( i => i), numberOfPartitions)

    //Part B
    val megaDataRDD = rowRDD.flatMap( r => {
      val random = new Random()

      val dataRange = (0 until numberOfRecords/numberOfPartitions).iterator
      dataRange.map[Row]( x => {
        val values = new mutable.ArrayBuffer[Any]
        for (i <- 0 until numberOfColumns) {
          if (i % 2 == 0) {
            values.+=(random.nextInt(100).toLong)
          } else {
            values.+=(random.nextInt(100).toString)
          }
        }
        new GenericRow(values.toArray)
      })
    })

    //Part C
    val schema =
      StructType(
        (0 until numberOfColumns).map( i => {
          if (i % 2 == 0) {
            StructField("longColumn_" + i, LongType, true) }
          else {
            StructField("stringColumn_" + i, StringType, true)
          }
        })
      )
    val df = sqlContext.createDataFrame(megaDataRDD, schema)
    df.saveAsParquetFile(outputPath)

    //Part D
    sc.stop()
  }
}
