package cn.spark.study.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object JsonDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ManuallySpecifyOptions")
      //.master("local")
      .getOrCreate()
    //创建DF,作为学生基本信息数据，写入hdfs,parquet文件
    val sc = spark.sparkContext
    val studentScoresDF = spark.read.json("hdfs://spark1:9000/students.json")
    studentScoresDF.createOrReplaceTempView("student_scores")
    //select出的rdd的元素都是Row类型的，row里的数值类型都是Long
    val goodStudentsDF= spark.sql("select name,score from student_scores where score >= 80")
    val goodStudentNames = goodStudentsDF.rdd.map(row=>row(0)).collect()

    val studentInfoJsons = Array("{\"name\":\"Leo\",\"age\":18}",
      "{\"name\":\"Marry\",\"age\":17}",
      "{\"name\":\"Jack\",\"age\":19}")

    val studentInfoJsonRDD = sc.parallelize(studentInfoJsons,3)
    val studentInfosDF = spark.read.json(studentInfoJsonRDD)

    studentInfosDF.createOrReplaceTempView("student_infos")
    var sql = "select name,age from student_infos where name in ("
    for(i <- 0 until  goodStudentNames.length){
      sql+="'"+goodStudentNames(i)+"'"
      if(i<goodStudentNames.length-1)
        sql+=','
    }
    sql+=")"

    val goodStudentInfoDF = spark.sql(sql)
    //join
    val goodStuRdd = goodStudentsDF.rdd
      .map(row=>(row.getAs[String]("name"),row.getAs[Long]("score").toInt))
      .join(goodStudentInfoDF.rdd
        .map(row=>(row.getAs[String]("name"),row.getAs[Long]("age").toInt))
      )

    val gooodStuRowsRdd = goodStuRdd.map(info=>
      Row(info._1
        ,info._2._1,
        info._2._2))
    //构造元数据
    val structType = StructType(
      Array(StructField("name",StringType,true),
        StructField("score",IntegerType,true),
        StructField("age",IntegerType,true)
      ))
    val goodStuDF = spark.createDataFrame(gooodStuRowsRdd,structType)

    goodStuDF.write.json("hdfs://spark1:9000/spark-study/good_students_scala");
  }

}
