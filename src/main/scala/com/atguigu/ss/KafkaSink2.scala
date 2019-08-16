package com.atguigu.ss

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019/8/14 7:39 PM
  */
object KafkaSink2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[1]")
            .appName("Test")
            .getOrCreate()
        import spark.implicits._
        
        val wordCount: DataFrame = spark.sparkContext.parallelize(Array("hello hello atguigu", "atguigu, hello"))
            .toDF("word")
            .groupBy("word")
            .count()
            .map(row => row.getString(0) + "," + row.getLong(1))
            .toDF("value")  // 写入数据时候, 必须有一列 "value"
        
        wordCount.write  // batch 方式
            .format("kafka")
            .option("kafka.bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092") // kafka 配置
            .option("topic", "update") // kafka 主题
            .save()
    }
}
