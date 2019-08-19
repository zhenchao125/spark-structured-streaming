package com.atguigu.realtime.app

import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-08-19 12:28
  *
  * 每天每地区每广告点击量实时统计top3
  *
  * 使用上个需求的结果, 减少城市维度即可.
  *
  */
object AdsClickCountTopApp {
    // 写入redis时的key的前缀   key: area:ads:top3:2019-03-22
    val keyPre = "area:ads:top3:"
    
    def statAdsClickCountTop3(spark: SparkSession) = {
        val df2 = spark.sql(
            """
              |select
              | dayString,
              | area,
              | adsId,
              | count(1) count
              |from tb_ads_info
              |group by dayString, area, adsId
            """.stripMargin)
            
         
        
    }
}
