package cn.spark.study.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetMergeSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ManuallySpecifyOptions")
      //.master("local")
      .getOrCreate()
    //创建DF,作为学生基本信息数据，写入hdfs,parquet文件
    val studentWithNameAge = Array(("leo",23),("lili",10))
    val sc = spark.sparkContext
    import spark.implicits._
    val studentWithNameAgeDF = sc.parallelize(studentWithNameAge,1)
      .toDF("name","age") //toDF要使用隐式转换
    studentWithNameAgeDF.write.mode(SaveMode.Append)
      .parquet("hdfs://spark1:9000/spark-study/students") //students是目录

    //创建第二个DF,学生成绩，写入一个parquet
    val studentWithNameScore = Array(("leo","A"),("tom","C"))
    val studentWithNameScoreDF =  sc.parallelize(studentWithNameScore,1)
      .toDF("name","score")

    studentWithNameScoreDF.write.mode(SaveMode.Append)
      .parquet("hdfs://spark1:9000/spark-study/students")

    //两个df的元数据不一样
    //所以这里期望的表数据，自动合并两个df:name，age score
    //mergeSchema读取students的数据，进行元数据合并
    val studentsDF = spark.read.option("mergeSchema","true")
      .parquet("hdfs://spark1:9000/spark-study/students")

    studentsDF.printSchema()
    studentsDF.show()
  }

}
