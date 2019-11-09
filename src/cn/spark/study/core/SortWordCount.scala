package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object SortWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortWordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/spark.txt")

    val words = lines.flatMap(line=>line.split(" "))
    val pairs = words.map(word=>(word,1))
    val wordCounts = pairs.reduceByKey(_+_)
    val countWords = wordCounts.map(wordCount=>(wordCount._2,wordCount._1))
    val sortedCountWords = countWords.sortByKey(false)
    sortedCountWords.foreach(countWord=>println(countWord._2+":"+countWord._1))
  }

}
