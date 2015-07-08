package com.cloudera.sa.examples.tablestats.model

/**
 * Created by ted.malaska on 6/29/15.
 */
class ColumnStats(var nulls:Long = 0l,
                  var empties:Long = 0l,
                  var totalCount: Long = 0l,
                  var uniqueValues:Long = 0l,
                  var maxLong:Long = Long.MinValue,
                  var minLong:Long = Long.MaxValue,
                  var sumLong:Long = 0l,
                  val topNValues:TopNList = new TopNList(10)) extends Serializable {

  def avgLong: Long = sumLong/totalCount

  //Part C.B
  def +=(colValue: Any, colCount: Long): Unit = {
    totalCount += colCount
    uniqueValues += 1

    if (colValue == null) {
      nulls += 1
    } else if (colValue.isInstanceOf[String]) {
      val colStringValue = colValue.asInstanceOf[String]
      if (colStringValue.isEmpty) {
        empties += 1
      }
    } else if (colValue.isInstanceOf[Long]) {
      val colLongValue = colValue.asInstanceOf[Long]
      if (maxLong < colLongValue) maxLong = colLongValue
      if (minLong > colLongValue) minLong = colLongValue
      sumLong += colLongValue
    }

    topNValues.add(colValue, colCount)
  }

  //Part C.C
  def +=(columnStats: ColumnStats): Unit = {
    totalCount += columnStats.totalCount
    uniqueValues += columnStats.uniqueValues
    nulls += columnStats.nulls
    empties += columnStats.empties
    sumLong += columnStats.sumLong
    maxLong = maxLong.max(columnStats.maxLong)
    minLong = minLong.max(columnStats.minLong)

    columnStats.topNValues.topNCountsForColumnArray.foreach{ r =>
      topNValues.add(r._1, r._2)
    }
  }

  override def toString = s"ColumnStats(nulls=$nulls, empties=$empties, totalCount=$totalCount, uniqueValues=$uniqueValues, maxLong=$maxLong, minLong=$minLong, sumLong=$sumLong, topNValues=$topNValues, avgLong=$avgLong)"
}

