package com.travel.listener

import java.util.logging.Logger

import com.alibaba.fastjson.JSON
import com.travel.common.JedisUtil
import com.travel.utils.TimeUtils
import org.apache.spark.streaming.scheduler._
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * @author wangyuxuan
 * @date 2020/3/14 10:22 下午
 * @description sparkStreaming的任务监控
 */
class SparkStreamingListener(duration: Int) extends StreamingListener {
  private val log: Logger = Logger.getLogger("sparkStreamingLogger")

  private val mapDatas = new mutable.HashMap[String, String]()

  private var finish_batchNum: Int = 0

  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {

    log.info("startingStream" + TimeUtils.formatDate(System.currentTimeMillis(),
      "yyyy-MM-dd HH:mm:ss"))
  }

  /**
   * 开始处理批次数据
   *
   * @param batchStarted
   */
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    val scheduleDelay: String = batchStarted.batchInfo.schedulingDelay.get.toString
    //统计任务解析时间

    //调度延迟时间为
    mapDatas.put("scheduleDelay", scheduleDelay)
  }

  /** *
   * 提交一批次数据
   *
   * @param batchSubmitted
   */
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {


    //数据提交数量
    val records: Long = batchSubmitted.batchInfo.numRecords
    mapDatas.put("submitRecords", records.toString)
  }

  /**
   * 批次数据完成时候的回调
   *
   * @param batchCompleted
   */
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val jedis: Jedis = JedisUtil.getJedis

    //该批次完成调用
    //该批次数据处理总时间
    val totalDelay: String = batchCompleted.batchInfo.totalDelay.get.toString
    //完成数量
    val records: Long = batchCompleted.batchInfo.numRecords
    val processingStratTime: Long = batchCompleted.batchInfo.processingStartTime.get
    val processingEndTime: Long = batchCompleted.batchInfo.processingEndTime.get
    mapDatas.put("totalDelay", totalDelay)
    mapDatas.put("records", records + "")
    mapDatas.put("processingStratTime", processingStratTime + "")
    mapDatas.put("processingEndTime", processingEndTime + "")
    //该批次是否出现阻塞
    if (duration * 6 < totalDelay.toLong * 1000) {
      log.info("流处理程序出现阻塞")
      //发送邮件
    }
    //完成的批次
    finish_batchNum = finish_batchNum + 1
    mapDatas.put("finish_batchNum", finish_batchNum + "")
    val jsonStr: String = JSON.toJSONString(mapDatas)
    jedis.set("batchMessage", jsonStr)
    jedis.expire("batchMessage", 3600)
    JedisUtil.returnJedis(jedis)
  }

  //接收数据异常
  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    val active: Boolean = receiverError.receiverInfo.active
    val id: String = receiverError.receiverInfo.executorId
    val message: String = receiverError.receiverInfo.lastErrorMessage
    val streamId: Int = receiverError.receiverInfo.streamId
    mapDatas.put("receiveError", s"发生错误的executorId为：${id}, 错误消息为：${message},当前流程序是否运行：${active}")
    val jedis: Jedis = JedisUtil.getJedis
    val jsonStr: String = JSON.toJSONString(mapDatas)
    jedis.set("receiveErrorMsg", jsonStr)
    jedis.expire("receiveErrorMsg", 3600)
    JedisUtil.returnJedis(jedis)
  }
}
