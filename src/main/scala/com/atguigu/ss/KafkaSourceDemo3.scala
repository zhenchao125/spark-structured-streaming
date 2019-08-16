package com.atguigu.ss

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019/8/13 10:23 AM
  */
object KafkaSourceDemo3 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("KafkaSourceDemo")
            .getOrCreate()
        import spark.implicits._
        
        val lines: Dataset[String] = spark.read  // 使用 read 方法,而不是 readStream 方法
            .format("kafka") // 设置 kafka 数据源
            .option("kafka.bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
            .option("subscribe", "topic1")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load
            .selectExpr("CAST(value AS STRING)")
            .as[String]
        
        val query: DataFrame = lines.flatMap(_.split("\\W+")).groupBy("value").count()
        
        query.write   // 使用 write 而不是 writeStream
            .format("console")
            .save()
    }
}
