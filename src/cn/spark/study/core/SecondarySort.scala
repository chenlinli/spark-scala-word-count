package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object SecondarySort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondarySort").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/sort.txt",1)
    val pairs = lines.map(line=>{
      val splits = line.split(" ")
      new Tuple2(new SecondarySortKey(splits(0).toInt,splits(1).toInt),line)
    })

    val sortPairs = pairs.sortByKey()
    val sortedLines = sortPairs.map(sortPair=>sortPair._2)
    sortedLines.foreach(println(_))


  }

}
