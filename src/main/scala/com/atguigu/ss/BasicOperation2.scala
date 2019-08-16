package com.atguigu.ss

import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019/8/13 2:08 PM
  */
object BasicOperation2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("BasicOperation")
            .getOrCreate()
        import spark.implicits._
        
        val peopleSchema: StructType = new StructType()
            .add("name", StringType)
            .add("age", LongType)
            .add("sex", StringType)
        val peopleDF: DataFrame = spark.readStream
            .schema(peopleSchema)
            .json("/Users/lzc/Desktop/data")
        
        val peopleDS: Dataset[People] = peopleDF.as[People] // 转成 ds
        
        
        val df: Dataset[String] = peopleDS.filter(_.age > 20).map(_.name)
        df.writeStream
            .outputMode("append")
            .format("console")
            .start
            .awaitTermination()
        
        
    }
}

case class People(name: String, age: Long, sex: String)