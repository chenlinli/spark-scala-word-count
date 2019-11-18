package cn.spark.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HDFSWordCount {
 def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("").setMaster("local[2]")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.textFileStream("hdfs://spark1:9000/wordcount_dir")
    val words = lines.flatMap(line=>line.split(" "))
    val pairs = words.map(word=>(word,1))
    val wordConunts = pairs.reduceByKey(_+_)

    wordConunts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
