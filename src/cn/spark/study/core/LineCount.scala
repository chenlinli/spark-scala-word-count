package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
//隐式转换，有Tuple2的RDD会自动隐式转换为PairRDDFunction,并提供reduceBykey等方法
object LineCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCount").setMaster("local");
    val sc = new SparkContext(conf);
    val lines = sc.textFile("src/hello.txt")
    val pairs = lines.map(line=>(line,1))
    val lineCounts = pairs.reduceByKey(_+_)
    lineCounts.foreach(lineCount =>println(lineCount._1+" : "+lineCount._2))

  }
}
