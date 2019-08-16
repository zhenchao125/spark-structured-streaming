package com.atguigu.ss

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019/8/16 3:44 PM
  */
object TriggerDemo {
    def main(args: Array[String]): Unit = {
        
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("TriggerDemo")
            .getOrCreate()
        import spark.implicits._
        
        val lines: DataFrame = spark.readStream
            .format("socket") // 设置数据源
            .option("host", "localhost")
            .option("port", 10000)
            .load
        
        
        val df: Dataset[String] = lines.as[String]
        
        /*// 1. 默认触发器
        val query: StreamingQuery = df.writeStream
            .outputMode("append")
            .format("console")
            .start()
            // 2. 微批处理模式
        val query: StreamingQuery = df.writeStream
                .outputMode("append")
                .format("console")
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .start
        
        // 3. 只处理一次. 处理完毕之后会自动退出
        val query: StreamingQuery = df.writeStream
                .outputMode("append")
                .format("console")
                .trigger(Trigger.Once())
                .start()*/
        
        // 4. 持续处理
        val query: StreamingQuery = df.writeStream
            .outputMode("append")
            .format("console")
            .trigger(Trigger.Continuous("1 seconds"))
            .start
        query.awaitTermination()
    }
}
