package com.atguigu.ss

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019/8/12 10:56 AM
  */
object WordCount1 {
    def main(args: Array[String]): Unit = {
        // 1. 创建 SparkSession. 因为 ss 是基于 spark sql 引擎, 所以需要先创建 SparkSession
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("WordCount1")
            .getOrCreate()
        import spark.implicits._
        // 2. 从数据源(socket)中加载数据.
        val lines: DataFrame = spark.readStream
            .format("socket") // 设置数据源
            .option("host", "localhost")
            .option("port", 10000)
            .load
        lines.printSchema()
        
        // 3. 把每行数据切割成单词
        val words: Dataset[String] = lines.as[String].flatMap(_.split("\\W+"))
        
        // 4. 计算 word count
        val wordCounts: DataFrame = words.groupBy("value").count()
        
        // 5. 启动查询, 把结果打印到控制台
        val query: StreamingQuery = wordCounts.writeStream
            .outputMode("update")
            .format("console")
            .start
        query.awaitTermination()
    }
}
