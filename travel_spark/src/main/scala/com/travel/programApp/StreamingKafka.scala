package com.travel.programApp

import java.util.regex.Pattern

import com.travel.common.{ConfigUtil, Constants}
import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 获取kafka当中的数据，进行解析，将海口以及成都的数据全部都保存到hbase里面去
  * 并且将成都的经纬度信息，保存到redis里面去，供实时轨迹浏览查看的
  */
object StreamingKafka {


  def main(args: Array[String]): Unit = {

    val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    val topics = Array(ConfigUtil.getConfig(Constants.CHENG_DU_GPS_TOPIC),ConfigUtil.getConfig(Constants.HAI_KOU_GPS_TOPIC))
    val group:String = "gps_consum_group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",// earliest,latest,和none
      "enable.auto.commit" -> (false: java.lang.Boolean)  //设置成为false，自己来维护offset的值
    )






    //使用direct方式来进行消费，  最少一个线程
    // receiver方式 最少两个线程
    val sparkConf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("streamingKafka")


    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val context: SparkContext = sparkSession.sparkContext
    context.setLogLevel("WARN")

    /**
      * sparkContext: SparkContext, batchDuration: Duration
      */

    val streamingContext = new StreamingContext(context,Seconds(5))


    /**
      * ssc: StreamingContext,
      * locationStrategy: LocationStrategy,
      * consumerStrategy: ConsumerStrategy[K, V]
      */
    /**
      * pattern: ju.regex.Pattern,
      * kafkaParams: collection.Map[String, Object],
      * offsets: collection.Map[TopicPartition, Long]
      * TopicPartition:哪一个topic，哪一个partition
      * Long：表示当前partition的offset的值
      *
      */



    val conn: Connection = HbaseTools.getHbaseConn

    val admin: Admin = conn.getAdmin

    if(!admin.tableExists(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))){

      val hbaseoffsetstoretable = new HTableDescriptor(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))

      val descriptor = new HColumnDescriptor(Constants.HBASE_OFFSET_FAMILY_NAME)

      hbaseoffsetstoretable.addFamily(descriptor)

      admin.createTable(hbaseoffsetstoretable)
      admin.close()

    }


    val topicMap = new mutable.HashMap[TopicPartition,Long]()

    val table: Table = conn.getTable(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))

    for(eachTopic <- topics){
      val rowkey = group + ":" + eachTopic
      val result: Result = table.get(new Get(rowkey.getBytes()))

      val cells: Array[Cell] = result.rawCells()
      for(eachCell <- cells){

        //获取到了类名
        val str: String = Bytes.toString(CellUtil.cloneQualifier(eachCell))
        val topicAndPartition: Array[String] = str.split(":")

        //offset的值
        val strOffset: String = Bytes.toString(CellUtil.cloneValue(eachCell))

        val partition = new TopicPartition(topicAndPartition(1),topicAndPartition(2).toInt)
        topicMap += (partition -> strOffset.toLong)

      }

    }

   val finalConsumerStrategy =  if(topicMap.size > 0){
      //第二次去获取数据，没有保存offset，从hbas里面能去得到offset的值
      ConsumerStrategies.SubscribePattern[String,String](Pattern.compile("(.*)gps_topic"),kafkaParams,topicMap)
    }else{
      //第一次去获取数据，没有保存offset，从hbas里面取不到offset的值
      ConsumerStrategies.SubscribePattern[String,String](Pattern.compile("(.*)gps_topic"),kafkaParams)
    }




    //消费一次之后，需要更新offset的值到hbase里面去，第二次获取的时候，就需要从hbase获取offset的值

    //将offset保存到habse里面去，怎么设计hbase的表模型？？？



    val resultDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](streamingContext,LocationStrategies.PreferConsistent,finalConsumerStrategy)


    /**
      * streamingContext: StreamingContext, kafkaParams: Map[String, Object], topics: Array[String], group: String,matchPattern:String
      */

    /**
      * 作业，能不能将offse维护到redis里面去
      *
      */
    val resultDStream: InputDStream[ConsumerRecord[String, String]] = HbaseTools.getStreamingContextFromHBase(streamingContext,kafkaParams,topics,group,"(.*)gps_topic")

    //获取出来数据，保存到hbase，以及redis，并且更新offset值






  }

}
