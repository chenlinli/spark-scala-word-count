package cn.spark.study.core

import org.apache.avro.SchemaBuilder.ArrayBuilder
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.{Driver, Global}

object GroupTop3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondarySort").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/score.txt")
    val pairs = lines.map(line=>{val cs = line.split(" ")
      Tuple2(cs(0),cs(1).toInt)
    })

    val groupedClaz = pairs.groupByKey()
    val sortedScores = groupedClaz.map(classScores=>{
      val cname = classScores._1
      val scores = classScores._2.toArray
      val top3 = ArrayBuffer[Int]()
      var max ,second ,third = Int.MinValue
      for (ele<-scores){
        if(ele>max){
          third = second
          second = max
          max = ele
        }else if(ele>second){
          third =second
          second = ele
        }else if(ele>third){
          third = ele
        }else{

        }
      }
      top3+=(max,second,third)
      Tuple2(cname,top3)
    })
    RDD
    org.apache.spark.deploy.worker.Worker
    sortedScores.foreach(sortedScore=>println(sortedScore._1+":"+sortedScore._2.mkString(" ")))
  }

}
