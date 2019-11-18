package cn.spark.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowHotWord {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WindowHotWord").setMaster("local[2]")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    val searchLogsDStream = ssc.socketTextStream("spark1",9999)

    val searchWordsDStream = searchLogsDStream.map(log=>log.split(" ")(1))
    val searchWordPairsDStream = searchWordsDStream.map(searchWord=>(searchWord,1))

    val searchWordCountsDStream = searchWordPairsDStream.reduceByKeyAndWindow(
      (v1:Int,v2:Int) => v1+v2,  //函数
      Seconds(60),  //窗口大小
      Seconds(10)  //滑动间隔
    )

    val finalDStream = searchWordCountsDStream.transform(searchWordCountsRDD=>{
      val countWordRDD = searchWordCountsRDD.map(tuple=>(tuple._2,tuple._1))
      val sortCountWordsRDD = countWordRDD.sortByKey(false)
      val sortWordCountsRDD = sortCountWordsRDD.map(tuple=>(tuple._2,tuple._1))
      val top3Word = sortWordCountsRDD.take(3)
      for(tuple <- top3Word){
        println(tuple)
      }
      sortWordCountsRDD
    })
    finalDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
