package cn.spark.study.sql

import org.apache.spark.sql.SparkSession

/**
  * scala 反射实现rdd->dataframe
  * scala开发spark程序，还要基于反射的RDD到DF,必须使用extends App,
  * 不能使用main，否则会编译错误
  *
  */
object RDD2DataFrameReflection extends App {
    val spark = SparkSession
      .builder().appName("RDD2DataFrameReflection").master("local").getOrCreate()

    //反射：rdd->df，手动导入隐式转换
    import spark.implicits._

    case class Student(id:Int,name:String,age:Int)
    //普通的元素为case class的rdd
    val students = spark.read.textFile("src/students.txt")
      .map(line=>line.split(","))
      .map(arr=>Student(arr(0).toInt,arr(1),arr(2).toInt))

    //直接使用rddde toDF
    val studentsDF = students.toDF()

    //students.registerTempTable("students")//弃用
    studentsDF.createOrReplaceTempView("students")

    val teenagerDF = spark.sql("select * from students where age > 17")

    val teenagerRdd = teenagerDF.rdd

    //这里的row是和class定义的顺序一样的，scala与java不一样，
    teenagerRdd.map(row=>Student(row(0).toString.toInt,row(1).toString,row(2).toString.toInt))
      .collect().foreach(stu=>println(stu.id+" "+stu.name+" "+stu.age))

    //scala对row的使用更加丰富,getAs获取指定列明的列
    teenagerRdd.map(row=>Student(row.getAs[Int]("id"),row.getAs[String]("name"),row.getAs[Int]("age")))
      .collect().foreach(stu=>println(stu.id+" "+stu.name+" "+stu.age))

    //还可以根据row的getValuesMap获得指定的几列的值，返回map
     teenagerRdd.map(row=>{
        val map= row.getValuesMap[Any](Array("id","name","age"))
         Student(map("id").toString.toInt,map("name").toString,map("age").toString.toInt)
    }).collect().foreach(stu=>println(stu.id+" "+stu.name+" "+stu.age))

}