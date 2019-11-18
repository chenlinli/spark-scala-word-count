package cn.spark.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformBlackList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransformBlackList").setMaster("local[2]")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    val blackList = Array(("tom", true))
    val blackListRDD = ssc.sparkContext.parallelize(blackList, 5)

    val adsClickLogDStream = ssc.socketTextStream("spark1", 9999);
    val userAdsClickLogDStream = adsClickLogDStream.map(adsClickLog => (adsClickLog.split(" ")(1), adsClickLog))

    val validAdsClidkLogDStream = userAdsClickLogDStream.transform(
      userAdsClickLogRDD=> {
        val joinedRdd = userAdsClickLogRDD.leftOuterJoin(blackListRDD)

        val filteredRdd = joinedRdd.filter(tuple=>{
          println(tuple)
          if(tuple._2._2.getOrElse(false)) {
            false
          }else {
            true
          }
        })
        val validRdd = filteredRdd.map(tuple=> tuple._2._1)
        validRdd
      }
    )

    validAdsClidkLogDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
