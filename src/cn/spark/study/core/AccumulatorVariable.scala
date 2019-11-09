package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorVariable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorVariable").setMaster("local")
    val sc = new SparkContext(conf)

    val list = Array(1,2,3,4,5)
    //accumulator已经弃用：提供LongAcumulator和
    val sum = sc.longAccumulator("longaccmulator")
    val nums = sc.parallelize(list)
    nums.foreach(num=>sum.add(num)) //+=
    println(sum.value) //如果这里使用的是map(num=>sum.add(num))结果是：0:spark的lazy的计算模型 ,map：transformation不会在println执行
  }

}
