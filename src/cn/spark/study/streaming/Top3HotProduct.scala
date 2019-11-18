package cn.spark.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Top3HotProduct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3HotProduct").setMaster("local[2]")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))

    val productClickLogDS = ssc.socketTextStream("spark1",9999);

    val categoryProductPairsDStream = productClickLogDS.map(log=>{
      val split = log.split(" ")
      (split(2)+"_"+split(1),1)
    })

    val categoryProductCountsDS = categoryProductPairsDStream.reduceByKeyAndWindow(
      (v1:Int,v2:Int)=>v1+v2,Seconds(60),Seconds(10))

    categoryProductCountsDS.foreachRDD(categoryProductCountsRDD=>{
      val categoryProductCountRowRDd = categoryProductCountsRDD.map(tuple=>{
        val split = tuple._1.split("_")
        Row(split(0),split(1),tuple._2)
      })

      val structType = StructType(Array(StructField("category",StringType,true),
        StructField("product",StringType,true)
      ,StructField("count",IntegerType,true)))
      val spark = SparkSession.builder().config(categoryProductCountsRDD.context.getConf).enableHiveSupport().getOrCreate()
      val categoryProductCountDF = spark.createDataFrame(categoryProductCountRowRDd,structType)

      categoryProductCountDF.createOrReplaceTempView("product_click_log")

      val top3DF = spark.sql("select category,product,count from("+
      "select category,product,count,row_number() over (partition by category order by count desc) rank from product_click_log"
        +") tmp where rank<=3")

      top3DF.show()

    })

    ssc.start()
    ssc.awaitTermination()
  }

}
