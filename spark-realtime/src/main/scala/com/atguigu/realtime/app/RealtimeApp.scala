package com.atguigu.realtime.app

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
            .master("local[2]")
            .appName("RealtimeApp")
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        // 需求1: 黑名单
        val filteredAdsInfoDS: Dataset[AdsInfo] = BlackListApp.statBlackList(spark)
        // 需求2:
//        AdsClickCountApp.statAdsClickCount(spark, filteredAdsInfoDS)
        // 需求3:
//        AdsClickCountTopApp.statAdsClickCountTop3(spark)
        
        // 需求4:
        
        
    }
}
