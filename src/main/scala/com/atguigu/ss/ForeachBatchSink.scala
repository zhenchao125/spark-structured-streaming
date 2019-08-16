package com.atguigu.ss

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019/8/14 7:39 PM
  */
object ForeachBatchSink {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[2]")
            .appName("ForeachBatchSink")
            .getOrCreate()
        import spark.implicits._
        
        val lines: DataFrame = spark.readStream
            .format("socket") // 设置数据源
            .option("host", "localhost")
            .option("port", 10000)
            .load
        
        val wordCount: DataFrame = lines.as[String]
            .flatMap(_.split("\\W+"))
            .groupBy("value")
            .count()
        
        val props = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaa")
        
        
        val query = wordCount.writeStream
            .outputMode("update")
            .foreachBatch((df, batchId) => {
                if (df.count() != 0) {
                    df.cache()
                    df.write.json(s"./$batchId")
                    df.write.mode("overwrite").jdbc("jdbc:mysql://hadoop201:3306/ss", "word_count", props)
                }
            })
            .start()
        
        
        query.awaitTermination()
        
    }
}
