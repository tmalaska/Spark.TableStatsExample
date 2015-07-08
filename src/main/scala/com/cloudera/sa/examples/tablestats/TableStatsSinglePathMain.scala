package com.cloudera.sa.examples.tablestats

import com.cloudera.sa.examples.tablestats.model.{FirstPassStatsModel}
import org.apache.spark._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
 * Created by ted.malaska on 6/27/15.
 */
object TableStatsSinglePathMain {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("TableStatsSinglePathMain <inputPath>")
      return
    }

    val inputPath = args(0)
    val runLocal = (args.length == 2 && args(1).equals("L"))
    var sc:SparkContext = null

    if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      sc = new SparkContext("local", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName("TableStatsSinglePathMain")
      sc = new SparkContext(sparkConfig)
    }
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //Part A
    var df = sqlContext.parquetFile(inputPath)
    //Part B
    val firstPassStats = getFirstPassStat( df)
    //Part E
    println(firstPassStats)
    //Part F
    sc.stop()
  }

  def getFirstPassStat(df: DataFrame): FirstPassStatsModel = {
    val schema = df.schema

    //Part B.1
    val columnValueCounts = df.flatMap(r =>
      (0 until schema.length).map { idx =>
        //((columnIdx, cellValue), count)
        ((idx, r.get(idx)), 1l)
      }
    ).reduceByKey(_ + _) //This is like word count

    //Part C
    val firstPassStats = columnValueCounts.mapPartitions[FirstPassStatsModel]{it =>
      val firstPassStatsModel = new FirstPassStatsModel()
      it.foreach{ case ((columnIdx, columnVal), count) =>
        firstPassStatsModel += (columnIdx, columnVal, count)
      }
      Iterator(firstPassStatsModel)
    }.reduce { (a, b) => //Part D
      a += (b)
      a
    }

    firstPassStats
  }
}
