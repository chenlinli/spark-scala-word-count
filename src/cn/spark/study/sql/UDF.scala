package cn.spark.study.sql

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object UDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UDF").master("local")getOrCreate()

    val sc = spark.sparkContext

    val names = Array("Leo","Marry","Jack","Tom")
    val nameRDD = sc.parallelize(names)

    val nameRowRDD = nameRDD.map(name=>Row(name))
    val structType =StructType(Array(StructField("name",StringType,true)))
    val nameDF = spark.createDataFrame(nameRowRDD,structType)

    //注册names table
    nameDF.createOrReplaceTempView("names")
    //定义:匿名函数；注册自定义函数spark.udf.register（函数名，函数体）
    spark.udf.register("strLen",(str:String)=>str.length)
    //使用
    val lengths = spark.sql("select name,strLen(name) from names").javaRDD.collect()
    
    for(i<-0 until(lengths.size())){
      println(lengths.get(i))
    }
  }

}