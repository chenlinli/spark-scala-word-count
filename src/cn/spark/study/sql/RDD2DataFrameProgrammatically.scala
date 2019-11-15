package cn.spark.study.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object RDD2DataFrameProgrammatically extends App {

  val spark = SparkSession.builder().appName("RDD2DataFrameProgrammatically")
    .master("local").getOrCreate()
  //构造元素为Row的普通RDD
  val sc = spark.sparkContext
  //sc的textFile返回的才是JavaRDD,spark的read.textFile返回的是DataSet

  val studentsRDD  = sc.textFile("src/students.txt").map(line=>{
    val split = line.split(",")
    Row(split(0).toInt,split(1),split(2).toInt)
  })

  //元数据构造
  val structType = StructType(Array(
    StructField("id",IntegerType,true),
    StructField("name",StringType,true),
    StructField("age",IntegerType,true)
  ))

  //rdd->df
  val studentDF = spark.createDataFrame(studentsRDD,structType)

  studentDF.createOrReplaceTempView("students")
  val teenagerDf = spark.sql("select * from students where age<18")

  teenagerDf.rdd.collect().foreach(println(_))

}