package com.cloudera.sa.examples.tablestats.prefTests

import com.cloudera.sa.examples.tablestats.model.TopNList

import scala.collection.mutable.{Map => MutableMap}

/**
 * Created by ted.malaska on 7/3/15.
 */
object FrequentItems {

  def main(args: Array[String]): Unit = {
    val freqItemCounter = new FreqItemCounter(10)
    val topNList = new TopNList(10)

    val startTime = System.currentTimeMillis();

    (1 until 1000).foreach( r => {
      freqItemCounter.add(r, r % 10)
    })

    /*
    (1 until 100000000).foreach( r => {
      topNList.add(r, 1)
    })
    */
    println("Finish Time:" + (System.currentTimeMillis() - startTime))
  }

  class FreqItemCounter(size: Int) extends Serializable {
    val baseMap: MutableMap[Any, Long] = MutableMap.empty[Any, Long]

    /**
     * Add a new example to the counts if it exists, otherwise deduct the count
     * from existing items.
     */
    def add(key: Any, count: Long): this.type = {
      if (baseMap.contains(key))  {
        baseMap(key) += count
      } else {
        if (baseMap.size < size) {
          baseMap += key -> count
        } else {
          // TODO: Make this more efficient... A flatMap?
          baseMap.retain((k, v) => v > count)
          baseMap.transform((k, v) => v - count)
        }
      }
      this
    }

    /**
     * Merge two maps of counts.
     * @param other The map containing the counts for that partition
     */
    def merge(other: FreqItemCounter): this.type = {
      other.baseMap.foreach { case (k, v) =>
        add(k, v)
      }
      this
    }
  }
}
