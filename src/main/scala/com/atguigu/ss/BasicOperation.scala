package com.atguigu.ss

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019/8/13 2:08 PM
  */
object BasicOperation {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("BasicOperation")
            .getOrCreate()
        val peopleSchema: StructType = new StructType()
            .add("name", StringType)
            .add("age", LongType)
            .add("sex", StringType)
        val peopleDF: DataFrame = spark.readStream
            .schema(peopleSchema)
            .json("/Users/lzc/Desktop/data")
        
        
        val df: DataFrame = peopleDF.select("name","age", "sex").where("age > 20") // 若类型 api
        df.writeStream
            .outputMode("append")
            .format("console")
            .start
            .awaitTermination()
    }
}
