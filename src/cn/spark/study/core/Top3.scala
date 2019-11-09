package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondarySort").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/top.txt")
    val numPair = lines.map(line => (line.toInt, 1))
    val sortedPairs = numPair.sortByKey(false)
    val sortedNums = sortedPairs.map(pair => pair._1)
    val array = sortedNums.take(3)
    for (elem <- array) {
      println(elem)
    }

  }
}