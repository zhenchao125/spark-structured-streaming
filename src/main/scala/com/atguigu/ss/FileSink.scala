package com.atguigu.ss

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019/8/14 7:39 PM
  */
object FileSink {
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
        
        val words: DataFrame = lines.as[String].flatMap(line => {
            line.split("\\W+").map(word => {
                (word, word.reverse)
            })
        }).toDF("原单词", "反转单词")
        
        words.writeStream
            .outputMode("append")
            .format("json") //  // 支持 "orc", "json", "csv"
            .option("path", "./filesink")
            .option("checkpointLocation", "./ck1")  // 必须指定 checkpoint 目录
            .start
            .awaitTermination()
    }
}
