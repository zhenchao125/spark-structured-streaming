package com.atguigu.realtime.app

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.RedisUtil
import org.apache.spark.sql._
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-08-19 09:37
  *
  * 需求1: 统计黑名单
  *
  * 其他需求直接使用过滤后的数据就可以了
  */
object BlackListApp {
    def statBlackList(spark: SparkSession): Dataset[AdsInfo] = {
        import spark.implicits._
        val dayStringFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val hmStringFormatter: SimpleDateFormat = new SimpleDateFormat("HH:mm")
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
                val date: Date = new Date(split(0).toLong)
                AdsInfo(split(0).toLong, new Timestamp(split(0).toLong), dayStringFormatter.format(date), hmStringFormatter.format(date), split(1), split(2), split(3), split(4))
            })
            .mapPartitions(adsInfoIt => { // 每个分区连接一次到redis读取黑名单, 然后把进入黑名单用户点击记录过滤掉
                val adsInfoList: List[AdsInfo] = adsInfoIt.toList
                // 1. 先读取到黑名单
                val client: Jedis = RedisUtil.getJedisClient
                val blackList: java.util.Set[String] = client.smembers(s"day:blcklist:${adsInfoList(0).dayString}")
                // 2. 过滤
                adsInfoList.filter(adsInfo => {
                    !blackList.contains(adsInfo.userId)
                }).toIterator
            })
            .withWatermark("timestamp", "24 hours") // 黑名单数据应该每天重新统计, 所以把水印设计成24小时
        
        // 创建临时表: tb_ads_info
        adsInfoDS.createOrReplaceTempView("tb_ads_info")
        
        // 需求1: 黑名单 每天每用户每广告的点击量
        // 2.  按照每天每用户每id分组, 然后计数, 计数超过阈值(100)的查询出来
        val result: DataFrame = spark.sql(
            """
              |select
              | dayString,
              | userId
              |from  tb_ads_info
              |group by dayString, userId, adsId
              |having count(1) >= 1000000
            """.stripMargin)
        // 3. 把点击量超过 100 的写入到redis中.
        result.writeStream
            .outputMode("update")
            .foreach(new ForeachWriter[Row] {
                var client: Jedis = _
                
                override def open(partitionId: Long, epochId: Long): Boolean = {
                    // 打开到redis的连接
                    client = RedisUtil.getJedisClient
                    client != null
                }
                
                override def process(value: Row): Unit = {
                    // 写入到redis  把每天的黑名单写入到set中  key: "day:blacklist" value: 黑名单用户
                    val dayString: String = value.getString(0)
                    val userId: String = value.getString(1)
                    client.sadd(s"day:blcklist:$dayString", userId)
                }
                
                override def close(errorOrNull: Throwable): Unit = {
                    // 关闭到redis的连接
                    if (client != null) client.close()
                }
            })
            .option("checkpointLocation", "C:/blacklist")
            .start()
        
        // 4. 把过滤后的数据返回   在其他地方也可以使用临时表: tb_ads_info
        adsInfoDS
    }
}
