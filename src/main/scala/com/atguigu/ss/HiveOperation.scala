package com.atguigu.ss

import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author lzc
  * Date 2019/8/13 2:08 PM
  */
object HiveOperation {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("HiveOperation")
            .enableHiveSupport()
            .getOrCreate()
        
        
        
        
    }
}
