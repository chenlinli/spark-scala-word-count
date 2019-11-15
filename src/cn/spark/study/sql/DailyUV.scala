package cn.spark.study.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DailyUV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("").master("local").getOrCreate();
    val sc = spark.sparkContext

    //这里要使用内置函数要导入sqlcontext下的隐式转换
    import spark.implicits._
    //构造用户访问日志数据--》DF
    val userAccessLog = Array("2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1125",
      "2015-10-02,1122",
      "2015-10-02,1121",
      "2015-10-02,1123")

    val userAccessLogRDD =  sc.parallelize(userAccessLog)
    //将rdd-->DF
    val userAccessLogRowRDD = userAccessLogRDD.map(log=>{
      val split = log.split(",");
      Row(split(0),split(1).toInt)})
    //构造元数据
    val structType =StructType(Array(
      StructField("date",StringType,true),
      StructField("userid",IntegerType,true)
    ))

    val userAccessLogDF = spark.createDataFrame(userAccessLogRowRDD,structType)
    //使用内置函数，
    //一天内用户访问多次，uv是用户去重后的访问总数
    //单引号作为前缀
    val rest = userAccessLogDF.groupBy("date")
      .agg('date,countDistinct('userid)) //按照date分组,每一组useid去重统计总数
      .javaRDD.collect()

    for( i <- 0 until rest.size()){
      println(rest.get(i))
    }
  }


}
