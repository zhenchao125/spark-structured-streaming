package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019-08-19 12:28
  *
  * 每天每地区每广告点击量实时统计top3
  *
  */
object AdsClickCountTopApp {
    // 写入redis时的key的前缀   key: area:ads:top3:2019-03-22
    val keyPre = "area:ads:top3:"
    
    def statAdsClickCountTop3(spark: SparkSession,filteredAdsInfoDS: Dataset[AdsInfo]): Unit = {
        /*val df2 = spark.sql(
            """
              |select
              | dayString,
              | area,
              | adsId,
              | count(1) count
              |from tb_ads_info
              |group by dayString, area, adsId
            """.stripMargin)*/
        /*val df2 = spark.sql(
            """
              |select
              | *,
              | rank() window(timestamp, 10 minutes, 10 minutes )
              |from tb_ads_info
            """.stripMargin)*/
       /* val df2 = spark.sql(
            """
              |select
              |    dayString,
              |    area,
              |    adsId,
              |    count,
              |    rank() over(partition by dayString, adsId sort by count desc) rank
              |
              |from(
              |    select
              |     dayString,
              |     area,
              |     adsId,
              |     count(1) count
              |    from tb_ads_info
              |    group by dayString, area, adsId
              |) f1
            """.stripMargin
        )*/
        import spark.implicits._
        import org.apache.spark.sql.functions._
        val df2 = filteredAdsInfoDS.groupBy(
            window($"timestamp", "24 hours", "24 hours"),
            $"dayString",
            $"area",
            $"adsId"
        ).count().orderBy("count")
        
        df2.writeStream
            .format("console")
            .outputMode("complete")
            .option("truncate", "false")
            .start
            .awaitTermination
    }
}
/*
f1:

select
 dayString,
 area,
 adsId,
 count(1) count
from tb_ads_info
group by dayString, area, adsId

select
    dayString,
    area,
    adsId,
    count,
    rank() over(partition by dayString, area, adsId sort by count desc) rank
    
from(
    select
     dayString,
     area,
     adsId,
     count(1) count
    from tb_ads_info
    group by dayString, area, adsId
) f1


 */