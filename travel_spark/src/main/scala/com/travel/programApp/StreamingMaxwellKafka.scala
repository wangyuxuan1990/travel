package com.travel.programApp

import java.lang

import com.travel.common.{ConfigUtil, Constants}
import com.travel.utils.{HbaseTools, JsonParse}
import org.apache.hadoop.hbase.client.Connection
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangyuxuan
 * @date 2020/3/11 14:18
 * @description 通过maxwell来解析mysql的binlog日志，
 *              实现了实时捕获mysql数据库当中的数据到kafka当中，
 *              然后我们就可以通过sparkStreaming程序来解析kafka数据进入hbase
 */
object StreamingMaxwellKafka {
  def main(args: Array[String]): Unit = {
    val brokers: String = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    val topics: Array[String] = Array(Constants.VECHE)
    val conf: SparkConf = new SparkConf().setAppName("StreamingMaxwellKafka").setMaster("local[1]")
    val group_id: String = "vech_group"
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> "earliest", // earliest,latest,和none
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    // 从kafka里面获取数据
    val getDataFromKafka: InputDStream[ConsumerRecord[String, String]] = HbaseTools.getStreamingContextFromHBase(ssc, kafkaParams, topics, group_id, "veche")

    getDataFromKafka.foreachRDD(eachRDD => {
      if (!eachRDD.isEmpty()) {
        // 循环遍历每一个partition
        eachRDD.foreachPartition(eachPartition => {
          val conn: Connection = HbaseTools.getHbaseConn
          eachPartition.foreach(eachLine => {
            // 获取到了我们一行的数 json格式的数据
            val line: String = eachLine.value()
            // 解析json字符串
            val tuple: (String, Any) = JsonParse.parse(line)
            // 将数据保存到hbase里面去
            HbaseTools.saveBusinessDatas(tuple._1, tuple, conn)
          })
          conn.close()
        })
      }
      // 更新offset的值
      val ranges: Array[OffsetRange] = eachRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      for (eachRange <- ranges) {
        val startOffset: Long = eachRange.fromOffset
        val endOffset: Long = eachRange.untilOffset
        val partition: Int = eachRange.partition
        val topic: String = eachRange.topic
        // 将offset的值保存到hbase里面去
        HbaseTools.saveBatchOffset(group_id, topic, partition + "", endOffset)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
