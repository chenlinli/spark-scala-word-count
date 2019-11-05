package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ActionOperation").setMaster("local")
    val sc = new SparkContext(conf)
//    reduce(sc)
//    collect(sc)
//    count(sc)
    countByKey(sc)
  }

  def reduce(sc:SparkContext): Unit ={
    val list = Array(1,2,3,4,5,6)
    val numbers = sc.parallelize(list)
    val sum = numbers.reduce(_+_)
    println(sum)
  }

  def collect(sc:SparkContext): Unit = {
    val list = Array(1, 2, 3, 4, 5, 6)
    val numbers = sc.parallelize(list)

    val doubleNums = numbers.map(num=>num*2)
    val array = doubleNums.collect()
    for (num<-array){
      println(num)
    }
  }

  def count(sc:SparkContext): Unit ={
    val list = Array(1,2,3,4,5,6)
    val numbers = sc.parallelize(list)
    val count = numbers.count()
    println(count)
  }

  def take(sc:SparkContext): Unit ={
    val list = Array(1,2,3,4,5,6)
    val numbers = sc.parallelize(list)
    val array = numbers.take(3)
    println(array.mkString(" "))
  }


  def countByKey(sc:SparkContext): Unit = {
    val stuList = Array(Tuple2("c1", 90), Tuple2("c3", 70), Tuple2("c2", 67), Tuple2("c3", 90))
    val stus = sc.parallelize(stuList, 1)
    val totalScores = stus.countByKey()

    println(totalScores)
  }
}
