package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local")

    val sc = new SparkContext(conf)

    val factor = 3
    val factorBroadcast = sc.broadcast(factor)
    val numarray = Array(1,3,4,5)
    val nums = sc.parallelize(numarray)
    val multipliedNums = nums.map(num=>num*factorBroadcast.value)
    multipliedNums.foreach(println(_))


  }

}
