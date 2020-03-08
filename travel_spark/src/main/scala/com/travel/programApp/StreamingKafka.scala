package com.travel.programApp

import java.lang
import java.util.regex.Pattern

import com.travel.common.{ConfigUtil, Constants, HBaseUtil}
import com.travel.loggings.Logging
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author wangyuxuan
 * @date 2020/3/7 10:36 下午
 * @description 获取kafka当中的数据，进行解析，将海口以及成都的数据全部都保存到hbase里面去
 *              并且将成都的经纬度信息，保存到redis里面去，供实时轨迹浏览查看的
 */
object StreamingKafka extends Logging {
  def main(args: Array[String]): Unit = {
    val brokers: String = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    val topics: Array[String] = Array(ConfigUtil.getConfig(Constants.CHENG_DU_GPS_TOPIC), ConfigUtil.getConfig(Constants.HAI_KOU_GPS_TOPIC))
    val group: String = "gps_consum_group"
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest", // earliest,latest,和none
      "enable.auto.commit" -> (false: lang.Boolean) //设置成为false，自己来维护offset的值
    )

    // 使用direct方式来进行消费 最少一个线程
    // receiver方式 最少两个线程
    val conf: SparkConf = new SparkConf().setAppName("StreamingKafka").setMaster("local[1]")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    val conn: Connection = HBaseUtil.getConnection
    val admin: Admin = conn.getAdmin
    if (!admin.tableExists(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))) {
      val hbaseoffsetstoretable: HTableDescriptor = new HTableDescriptor(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))
      val descriptor: HColumnDescriptor = new HColumnDescriptor(Constants.HBASE_OFFSET_FAMILY_NAME)
      hbaseoffsetstoretable.addFamily(descriptor)
      admin.createTable(hbaseoffsetstoretable)
      admin.close()
    }

    val topicMap: mutable.HashMap[TopicPartition, Long] = new mutable.HashMap[TopicPartition, Long]()
    val table: Table = conn.getTable(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))
    for (eachTopic <- topics) {
      val rowKey: String = group + ":" + eachTopic
      val result: Result = table.get(new Get(rowKey.getBytes()))
      val cells: Array[Cell] = result.rawCells()
      for (eachCell <- cells) {
        // 获取到了列名
        val str: String = Bytes.toString(CellUtil.cloneQualifier(eachCell))
        val topicAndPartition: Array[String] = str.split(":")
        // offset的值
        val strOffset: String = Bytes.toString(CellUtil.cloneValue(eachCell))
        val topicPartition: TopicPartition = new TopicPartition(topicAndPartition(1), topicAndPartition(2).toInt)
        topicMap += (topicPartition -> strOffset.toLong)
      }
    }

    val finalConsumerStrategy: ConsumerStrategy[String, String] = if (topicMap.size > 0) {
      // 第二次去获取数据，没有保存offset，从hbase里面能去得到offset的值
      ConsumerStrategies.SubscribePattern[String, String](Pattern.compile("(.*)gps_topic"), kafkaParams, topicMap)
    } else {
      // 第一次去获取数据，没有保存offset，从hbase里面取不到offset的值
      ConsumerStrategies.SubscribePattern[String, String](Pattern.compile("(.*)gps_topic"), kafkaParams)
    }

    // 消费一次之后，需要更新offset的值到hbase里面去，第二次获取的时候，就需要从hbase获取offset的值

    // 将offset保存到hbase里面去，怎么设计hbase的表模型？？？

    val resultDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      finalConsumerStrategy
    )
  }
}
