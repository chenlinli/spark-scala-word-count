package cn.spark.study.sql

import org.apache.spark.sql.SparkSession

object ParquetLoadData extends App {
  val spark = SparkSession.builder().appName("ManuallySpecifyOptions")
    //.master("local")
    .getOrCreate()
  val userDF = spark.read.parquet("hdfs://spark1:9000/users.parquet")
  userDF.createOrReplaceTempView("users")
  val nameDF = spark.sql("select name from users")
  nameDF.rdd.map(row=>"name:"+row(0)).collect()
    .foreach(println(_))

}