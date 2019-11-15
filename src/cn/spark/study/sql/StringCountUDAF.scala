package cn.spark.study.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
  * 自定义聚合函数
  */
class StringCountUDAF extends UserDefinedAggregateFunction{
  //输入数据类型
  override def inputSchema: StructType = StructType(Array(StructField("str",StringType,true)))

  //聚合时处理的数据类型
  override def bufferSchema: StructType = StructType(Array(StructField("count",IntegerType,true)))

  //函数返回值类型
  override def dataType: DataType =  IntegerType

  override def deterministic: Boolean = true

  //为每个分组数据执行初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  //每个分组有新的值进来如何对分组计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getAs[Int](0)+1
  }

  //由于分布式，一个分组的数据可能在不同节点update,最后各个节点的值要merge
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getAs[Int](0)+buffer2.getAs[Int](0)
  }

  //一个分组的聚合值，如何通过中间的缓存聚合值，最后返回一个最终的聚合值;eg聚合值*2，平均值最终/个数
  override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
}