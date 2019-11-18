package cn.spark.study.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[2]")

    //创建StreamingContext
    val ssc = new StreamingContext(conf,Seconds(1))
    val lines = ssc.socketTextStream("localhost",9999);
    val words = lines.flatMap(line=>line.split(" "))
    val pairs = words.map(word=>(word,1))
    val wordCounts = pairs.reduceByKey(_+_)
    Thread.sleep(5000)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination();
  }

}
