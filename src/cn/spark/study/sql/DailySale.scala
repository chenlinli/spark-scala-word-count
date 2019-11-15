package cn.spark.study.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.spark.sql.functions._
object DailySale {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("").master("local").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    //统计网站登录用户的销售额统计,有时出现日志的丢失用户信息，不与统计
    val userSaleLog = Array("2015-10-01,55.06,1122",
      "2015-10-01,55,1133",
      "2015-10-03,59.06,",
      "2015-10-03,105.06,1144",
      "2015-10-02,65.06,1123",
      "2015-10-02,87.06,1133")
    val userSaleLogRDD = sc.parallelize(userSaleLog, 5)

    //过滤有效日志
    val filteduserSaleLogRDD = userSaleLogRDD.filter(log => log.split(",").length == 3)

    val saleLogRowRDD = filteduserSaleLogRDD.map(log => {
      val split = log.split(",")
      Row(split(0), split(1).toDouble, split(2).toInt)
    })

    val structType = StructType(Array(StructField("date", StringType, true),
      StructField("sale", DoubleType, true),
      StructField("userid", IntegerType, true)
    ))

    val saleLogDF = spark.createDataFrame(saleLogRowRDD, structType)

    //每日销售额统计
    val sumSale = saleLogDF.groupBy("date")
      .agg('date, sum('sale))
      .javaRDD
      .collect()

    for (i <- 0 until sumSale.size()) {
      println(sumSale.get(i))
    }
  }
}