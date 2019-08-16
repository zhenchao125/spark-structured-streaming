package com.atguigu.ss

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019/8/13 10:23 AM
  */
object KafkaSourceDemo2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("KafkaSourceDemo")
            .getOrCreate()
        import spark.implicits._
        // 得到的 df 的 schema 是固定的: key,value,topic,partition,offset,timestamp,timestampType
        val lines: Dataset[String] = spark.readStream
            .format("kafka") // 设置 kafka 数据源
            .option("kafka.bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
            .option("subscribe", "topic1") // 也可以订阅多个主题:   "topic1,topic2"
            .load
            .selectExpr("CAST(value AS STRING)")
            .as[String]
        val query: DataFrame = lines.flatMap(_.split("\\W+")).groupBy("value").count()
        query.writeStream
            .outputMode("complete")
            .format("console")
            .option("checkpointLocation", "./ck1")  // 下次启动的时候, 可以从上次的位置开始读取
            .start
            .awaitTermination()
    }
}
