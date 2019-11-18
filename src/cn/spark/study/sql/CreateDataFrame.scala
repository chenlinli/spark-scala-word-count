package cn.spark.study.sql

import org.apache.spark.sql.{SQLContext, SparkSession}

object CreateDataFrame {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CreateDataFrame").getOrCreate()
    val df = spark.read.json("src/students.json")
    df.show()

    SQLContext
  }
}