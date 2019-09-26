package com.atguigu.realtime.app

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.realtime.bean.AdsInfo
import org.apache.spark.sql._


/**
  * Author lzc
  * Date 2019-08-17 17:52
  */
object RealtimeApp {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .appName("RealtimeApp")
            .master("local[*]")
            .getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("warn")
        // 从 kafka 读取数据, 为了方便后续处理, 封装数据到 AdsInfo 样例类中
        val dayStringFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val hmStringFormatter: SimpleDateFormat = new SimpleDateFormat("HH:mm")
        val adsInfoDS: Dataset[AdsInfo] = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
            .option("subscribe", "ads_log")
            .load
            .select("value")
            .as[String]
            .map(v => {
                val split: Array[String] = v.split(",")
                val date: Date = new Date(split(0).toLong)
                AdsInfo(split(0).toLong, new Timestamp(split(0).toLong), dayStringFormatter.format(date), hmStringFormatter.format(date), split(1), split(2), split(3), split(4))
            })
            .withWatermark("timestamp", "24 hours") // 都是统计每天的数据, 对迟到24小时的数据废弃不用
        
        
        // 需求1: 黑名单
        val filteredAdsInfoDS: Dataset[AdsInfo] = BlackListApp.statBlackList(spark, adsInfoDS)
        // 需求2:
        AdsClickCountApp.statAdsClickCount(spark, filteredAdsInfoDS)
        
        // 需求3:
        AdsClickCountTopApp.statAdsClickCountTop3(spark, filteredAdsInfoDS)
        
    }
}
