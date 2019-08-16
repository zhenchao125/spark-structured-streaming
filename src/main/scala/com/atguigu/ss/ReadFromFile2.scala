package com.atguigu.ss

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019/8/13 9:01 AM
  */
object ReadFromFile2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("ReadFromFile")
            .getOrCreate()
        
        // 定义 Schema, 用于指定列名以及列中的数据类型
        val userSchema: StructType = new StructType().add("name", StringType).add("sex", StringType).add("age", IntegerType)
        
        val user: DataFrame = spark.readStream
            .schema(userSchema)
            .csv("/Users/lzc/Desktop/csv")
        
        val query: StreamingQuery = user.writeStream
            .outputMode("append")
            .trigger(Trigger.ProcessingTime(0)) // 触发器 数字表示毫秒值. 0 表示立即处理
            .format("console")
            .start()
        query.awaitTermination()
    }
}
