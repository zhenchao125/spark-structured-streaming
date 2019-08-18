package com.atguigu.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.realtime.bean.AdsInfo
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * Author lzc
  * Date 2019-08-17 17:52
  */
object RealtimeApp {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("RealtimeApp")
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        import spark.implicits._
        
        // 1. 从 kafka 读取数据, 为了方便后续处理, 封装数据到 AdsInfo 样例类中
        val adsInfoDS: Dataset[AdsInfo] = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
            .option("subscribe", "ads_log")
            .load
            .select("value")
            .as[String]
            .map(v => {
                val split: Array[String] = v.split(",")
                val dayStringFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
                val hmStringFormatter: SimpleDateFormat = new SimpleDateFormat("HH:mm")
                val date: Date = new Date(split(0).toLong)
                AdsInfo(split(0).toLong, dayStringFormatter.format(date), hmStringFormatter.format(date), split(1), split(2), split(3), split(4))
            })
        
        // 创建临时表: tb_ads_info
        adsInfoDS.createOrReplaceTempView("tb_ads_info")
        
        // 需求1: 黑名单 每天每用户每广告的点击量
        // 1.1 查询出每天每用户每广告的点击量
        val result: DataFrame = spark.sql(
            """
              |select
              | dayString,
              | userId,
              | adsId,
              | count(1) count
              |from  tb_ads_info
              |group by dayString, userId, adsId
            """.stripMargin)
        // 1.2 把点击量超过100的写入到mysql中
        
        
        // 需求2:
        
        // 需求3:
        
        
        // 需求4:
        
        // 2. 测试是否消费成功
        result.writeStream
            .format("console")
            .outputMode("complete")
            .trigger(Trigger.ProcessingTime(1000))
            .option("truncate", "false")
            .start
            .awaitTermination
    }
}
