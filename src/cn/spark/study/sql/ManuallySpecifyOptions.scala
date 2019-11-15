package cn.spark.study.sql

import org.apache.spark.sql.SparkSession

object ManuallySpecifyOptions extends App{
  val spark = SparkSession.builder().appName("ManuallySpecifyOptions")
    //.master("local")
    .getOrCreate()

  val peopleDF = spark.read.format("json").load("hdfs://spark1:9000/people.json")
  peopleDF.select("name").write.format("parquet").save("hdfs://spark1:9000/peopleName_java")
}