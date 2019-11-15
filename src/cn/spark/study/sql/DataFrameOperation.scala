package cn.spark.study.sql

import org.apache.spark.sql.SparkSession

object DataFrameOperation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameOperation").getOrCreate()
    val df = spark.read.json("hdfs://spark1:9000/students.json")
    df.show()
    println("schema")
    df.printSchema()
    println("name")
    df.select("name").show()
    println("name,age+1")
    df.select(df("name"),df("age")+1).show()
    println("age>18")
    df.filter(df("age")>18).show()
    println("age count")
    df.groupBy("age").count().show()
  }
}