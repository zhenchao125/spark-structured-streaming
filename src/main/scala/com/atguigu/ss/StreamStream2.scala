package com.atguigu.ss

import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
/**
  * Author lzc
  * Date 2019/8/16 5:09 PM
  */
object StreamStream2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("StreamStream1")
            .getOrCreate()
        import spark.implicits._
        
        // 第 1 个 stream
        val nameSexStream: DataFrame = spark.readStream
            .format("socket")
            .option("host", "hadoop201")
            .option("port", 10000)
            .load
            .as[String]
            .map(line => {
                val arr: Array[String] = line.split(",")
                (arr(0), arr(1), Timestamp.valueOf(arr(2)))
            }).toDF("name1", "sex", "ts1")
            .withWatermark("ts1", "2 minutes")
        
        // 第 2 个 stream
        val nameAgeStream: DataFrame = spark.readStream
            .format("socket")
            .option("host", "hadoop201")
            .option("port", 20000)
            .load
            .as[String]
            .map(line => {
                val arr: Array[String] = line.split(",")
                (arr(0), arr(1).toInt, Timestamp.valueOf(arr(2)))
            }).toDF("name2", "age", "ts2")
            .withWatermark("ts2", "1 minutes")
        
        
        // join 操作
        val joinResult: DataFrame = nameSexStream.join(
            nameAgeStream,
            expr(
                """
                  |name1=name2 and
                  |ts2 >= ts1 and
                  |ts2 <= ts1 + interval 1 minutes
                """.stripMargin))
        
        joinResult.writeStream
            .outputMode("append")
            .format("console")
            .trigger(Trigger.ProcessingTime(0))
            .start()
            .awaitTermination()
    }
}
