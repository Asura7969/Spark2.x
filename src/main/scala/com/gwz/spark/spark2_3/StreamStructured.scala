package com.gwz.spark.spark2_3

import java.sql.Timestamp

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession

object StreamStructured {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(StreamStructured.getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._
    val dataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("subscribe", "topicName")
      .option("group.id","groupName")
      .load()
      .selectExpr("CAST(value AS STRING)")
      //.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
      .as[String]
      //.as[(String, String)]

    import org.apache.spark.sql.functions.{concat_ws,collect_list,window}
    val value = dataFrame.map(msg => {
      try {
        val splitIndex = msg.indexOf("{")
        val json = msg.substring(splitIndex)
        val jsonObject = JSON.parseObject(json)
        val condition = jsonObject.getJSONObject("condition")
        val reqid = condition.getString("reqid")
        val rpcid = condition.getString("rpcid")
        val timestamp = condition.getString("timestamp").toLong
        (reqid, new Timestamp(timestamp), rpcid)
      } catch {
        case ex:Exception => ("",new Timestamp(System.currentTimeMillis()),"")
      }
    }).filter(_._1 != "").toDF("reqid","timestamp","rpcid")

      //withWatermark 水印,即允许数据迟到的时间,超过改时间则直接抛弃
      val frame = value.withWatermark("timestamp", "10 minutes")
        .groupBy(window($"timestamp", "10 minutes", "1 minutes"),$"reqid")
        .count()

    val query = frame.writeStream

      /**
        * 三种输出模式
        * 1、complete：整个结果表都将被写入到外部的存储器
        * 2、append:只有自上次触发执行后在结果表中附加的新行会被写入外部存储器
        * 3、update:只有自上次触发执行后在结果表中更新的行将被写入到外部存储器
        */
      .outputMode("complete")
      .format("console")
      //为了在程序重启之后可以接着上次的执行结果继续执行，需要设置检查点
      .option("checkpointLocation","path/to/HDFS/dir")
      //完全显示结果表的列和值
      .option("truncate","false")
      .start()

    //frame.foreach(...)  也可自己实现外部存储方式

    query.awaitTermination()
  }
}
