package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
object TransformationOperation {
  def main(args: Array[String]): Unit = {
    val conf =  new SparkConf().setAppName("LineCount").setMaster("local");
    val sc =  new SparkContext(conf);
//    map(sc)
//    filter(sc)
//    flatMap(sc)
    //groupByKey(sc)
//    reduceByKey(sc)
//    sortByKey(sc)
//    join(sc)
    cogroup(sc)
  }

  def map(sc:SparkContext): Unit ={
    val numbers = Array(1,2,3,4,5)
    val numbersRDD = sc.parallelize(numbers,1)
    val multipliedRdd = numbersRDD.map(num=>num*2)
    multipliedRdd.foreach(println(_))
  }

  def filter(sc:SparkContext): Unit ={
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD = sc.parallelize(numbers)
    val evenRDD = numberRDD.filter(num=>num%2==0)
    evenRDD.foreach(println(_))
  }

  def flatMap(sc:SparkContext): Unit ={
    val lineList = Array("hello you","hello me","hello world")
    val lines = sc.parallelize(lineList)
    val words = lines.flatMap(line=>line.split(" "))
    words.foreach(println(_))
  }

  def groupByKey(sc:SparkContext): Unit ={
    val scoreList = Array(Tuple2("c1",90),Tuple2("c3",70),Tuple2("c2",67),Tuple2("c3",90))
    val scores = sc.parallelize(scoreList,1)
    val groupedScores = scores.groupByKey()
    groupedScores.foreach(classScores=> {
      println(classScores._1)
      classScores._2.foreach(singleScores=>println(singleScores))
      println()
    })
  }

  def reduceByKey(sc:SparkContext): Unit ={
    val scoreList = Array(Tuple2("c1",90),Tuple2("c3",70),Tuple2("c2",67),Tuple2("c3",90))
    val scores = sc.parallelize(scoreList,1)
    val totalScores = scores.reduceByKey(_+_)
    totalScores.foreach(classScore=>println(classScore._1+" : "+classScore._2))
  }
  def sortByKey(sc:SparkContext): Unit ={
    val scoreList = Array(Tuple2(90,"tom"),Tuple2(80,"marry"),Tuple2(56,"jhon"),Tuple2(90,"jack"))
    val scores = sc.parallelize(scoreList,1)//始终一样
    val sortedScores = scores.sortByKey() //默认升序，false:降序
    sortedScores.foreach(scoreStu=>println(scoreStu._1+":"+scoreStu._2))

  }

  def join(sc:SparkContext): Unit ={
    val stuList = Array(Tuple2(1, "leo"), Tuple2(2, "jack"), Tuple2(3, "tom"))
    val scoreList = Array(Tuple2(1, 100), Tuple2(2, 77),  Tuple2(3, 88))

    val stus= sc.parallelize(stuList);
    val scores = sc.parallelize(scoreList)

    val stuScores = stus.join(scores)

    stuScores.foreach(stuScore=>{
      println(stuScore._1+":<"+stuScore._2._1+","+stuScore._2._2+">")
    })

  }


  def cogroup(sc:SparkContext): Unit ={
    val stuList = Array(Tuple2(1, "leo"), Tuple2(2, "jack"), Tuple2(3, "tom"))
    val scoreList = Array(
       Tuple2(1,100),
       Tuple2(2,77),
       Tuple2(3,88),
       Tuple2(3,50),
       Tuple2(2,89),
       Tuple2(1,78)
    )
    val stus= sc.parallelize(stuList);
    val scores = sc.parallelize(scoreList)

    val stuScores = stus.cogroup(scores)
    stuScores.foreach(stuScore=>{
      println(stuScore._1+":<"+stuScore._2._1+","+stuScore._2._2+">")
    })

  }
}
