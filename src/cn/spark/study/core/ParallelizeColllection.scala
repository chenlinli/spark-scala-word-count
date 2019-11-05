package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeColllection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParallelizeColllection").setMaster("local")

    val sc = new SparkContext(conf)

    val numbers = Array(1,2,3,4,5,6,7,8,9,10)

    val numbersRdd = sc.parallelize(numbers,5) //partition个数

    val sum = numbersRdd.reduce(_+_)

    println(sum)
  }
}
