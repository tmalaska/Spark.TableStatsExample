package com.cloudera.sa.examples.tablestats.model

import scala.collection.mutable

/**
 * Created by ted.malaska on 6/29/15.
 */
class TopNList(val maxSize:Int)  extends Serializable {
  val topNCountsForColumnArray = new mutable.ArrayBuffer[(Any, Long)]
  var lowestColumnCountIndex:Int = -1
  var lowestValue = Long.MaxValue

  def add(newValue:Any, newCount:Long): Unit = {
    if (topNCountsForColumnArray.length < maxSize -1) {
      topNCountsForColumnArray += ((newValue, newCount))
    } else if (topNCountsForColumnArray.length == maxSize) {
      updateLowestValue
    } else {
      if (newCount > lowestValue) {
        topNCountsForColumnArray.insert(lowestColumnCountIndex, (newValue, newCount))
        updateLowestValue
      }
    }
  }

  def updateLowestValue: Unit = {
    var index = 0

    topNCountsForColumnArray.foreach{ r =>
      if (r._2 < lowestValue) {
        lowestValue = r._2
        lowestColumnCountIndex = index
      }
      index+=1
    }
  }


  override def toString = s"TopNList(topNCountsForColumnArray=$topNCountsForColumnArray)"
}
