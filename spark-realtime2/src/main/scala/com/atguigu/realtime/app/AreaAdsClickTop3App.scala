package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.uitl.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object AreaAdsClickTop3App {
    def statAreaClickTop3(adsInfoDStream: DStream[AdsInfo]) = {
        
        // 1. 每天每地区每广告的点击率
        val dayAreaCount: DStream[((String, String), (String, Int))] = adsInfoDStream
            .map(adsInfo => ((adsInfo.dayString, adsInfo.area, adsInfo.adsId), 1)) // ((天, 地区, 广告), 1)
            .updateStateByKey((seq: Seq[Int], option: Option[Int]) => Some(seq.sum + option.getOrElse(0))) // ((天, 地区, 广告), 1000)
            .map {
                case ((day, area, adsId), count) =>
                    ((day, area), (adsId, count))
            }
        
        // 2. 按照 (天, 地区) 分组, 然后每组内排序, 取 top3
        val dayAreaAdsClickTop3: DStream[(String, String, List[(String, Int)])] = dayAreaCount
            .groupByKey
            .map {
                case ((day, area), adsCountIt) =>
                    (day, area, adsCountIt.toList.sortBy(-_._2).take(3))
                
            }
        
        // 3. 写入到redis
        dayAreaAdsClickTop3.foreachRDD(rdd => {
            // 建立到 redis 的连接
            val jedisClient: Jedis = RedisUtil.getJedisClient
            val arr: Array[(String, String, List[(String, Int)])] = rdd.collect
            // 写到 redis:  key-> "area:das:top3:"+2019-09-25   value:
            //                                                  field               value
            //                                                  {东北:  {3: 1000, 2:800, 10:500}  }
            arr.foreach{
                case (day, area, adsIdCountList) => {
                    import org.json4s.JsonDSL._
                    // list 结合转成 json 字符串
                    val adsCountJsonString = JsonMethods.compact(JsonMethods.render(adsIdCountList))
                    jedisClient.hset(s"area:day:top3:$day", area, adsCountJsonString)
                }
            }
            
            jedisClient.close()
        })
        
    }
}
