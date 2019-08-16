package com.atguigu.ss

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019/8/13 10:23 AM
  */
object KafkaSourceDemo {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("KafkaSourceDemo")
            .getOrCreate()
        
        // 得到的 df 的 schema 是固定的: key,value,topic,partition,offset,timestamp,timestampType
        val df: DataFrame = spark.readStream
            .format("kafka") // 设置 kafka 数据源
            .option("kafka.bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
            .option("subscribe", "topic1") // 也可以订阅多个主题:   "topic1,topic2"
            .load
        
        
        df.writeStream
            .outputMode("update")
            .format("console")
            .trigger(Trigger.Continuous(1000))
            .start
            .awaitTermination()
        
        
    }
}
