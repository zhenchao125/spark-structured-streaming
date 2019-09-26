package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019-09-06 17:49
  */
object LastHourAdsClickApp {
    def statLastHourAdsClick(spark: SparkSession, filteredAdsInfoDS: Dataset[AdsInfo]): Unit = {
    
    }
}
