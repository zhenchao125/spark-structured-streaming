package com.atguigu.ss

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019/8/14 7:39 PM
  */
object KafkaSink {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[1]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
        
        val lines: DataFrame = spark.readStream
            .format("socket") // 设置数据源
            .option("host", "localhost")
            .option("port", 10000)
            .load
        
        val words = lines.as[String]
                .flatMap(_.split("\\W+"))
                .groupBy("value")
                .count()
                .map(row => row.getString(0) + "," + row.getLong(1))
                .toDF("value")  // 写入数据时候, 必须有一列 "value"
        
        words.writeStream
            .outputMode("update")
            .format("kafka")
            .trigger(Trigger.ProcessingTime(0))
            .option("kafka.bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092") // kafka 配置
            .option("topic", "update") // kafka 主题
            .option("checkpointLocation", "./ck1")  // 必须指定 checkpoint 目录
            .start
            .awaitTermination()
    }
}
