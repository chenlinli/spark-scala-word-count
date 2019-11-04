package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")

    val sc = new SparkContext(conf)

    var lines = sc.textFile("hdfs://spark1:9000/spark.txt",1)

    val words = lines.flatMap(line=>line.split(" "))

    val pairs = words.map{word=>(word,1)}

    val wordcounts = pairs.reduceByKey{_+_}

    wordcounts.foreach(wordcount =>println(wordcount._1+":"+wordcount._2))

  }

}
