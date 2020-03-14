package com.travel.listener

import com.alibaba.fastjson.JSON
import com.travel.common.JedisUtil
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * @author wangyuxuan
 * @date 2020/3/14 10:24 下午
 * @description sparkSQL的任务监控
 */
class SparkSessionListener extends SparkListener {
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val jedis: Jedis = JedisUtil.getJedis

    val jedisMap = new mutable.HashMap[String, String]()

    val metrics: TaskMetrics = taskEnd.taskMetrics
    jedisMap.put("executorCpuTime", metrics.executorCpuTime + "")
    jedisMap.put("jvmGCTime", metrics.jvmGCTime + "")
    jedisMap.put("executorDeserializeCpuTime", metrics.executorDeserializeCpuTime + "")
    jedisMap.put("diskBytesSpilled", metrics.diskBytesSpilled + "")
    jedisMap.put("executorDeserializeTime", metrics.executorDeserializeTime + "")
    jedisMap.put("resultSize", metrics.resultSize + "")

    jedisMap.put("executorCpuTime", metrics.memoryBytesSpilled + "")
    jedisMap.put("executorCpuTime", metrics.resultSerializationTime + "")

    jedis.set("taskMetrics", JSON.toJSONString(jedisMap))

    //####################shuffle#######
    val shuffleReadMetrics = metrics.shuffleReadMetrics
    val shuffleWriteMetrics = metrics.shuffleWriteMetrics

    val shuffleMap = scala.collection.mutable.HashMap(
      "remoteBlocksFetched" -> shuffleReadMetrics.remoteBlocksFetched, //shuffle远程拉取数据块
      "localBlocksFetched" -> shuffleReadMetrics.localBlocksFetched,
      "remoteBytesRead" -> shuffleReadMetrics.remoteBytesRead, //shuffle远程读取的字节数
      "localBytesRead" -> shuffleReadMetrics.localBytesRead,
      "fetchWaitTime" -> shuffleReadMetrics.fetchWaitTime,
      "recordsRead" -> shuffleReadMetrics.recordsRead, //shuffle读取的记录总数
      "bytesWritten" -> shuffleWriteMetrics.bytesWritten, //shuffle写的总大小
      "recordsWritte" -> shuffleWriteMetrics.recordsWritten, //shuffle写的总记录数
      "writeTime" -> shuffleWriteMetrics.writeTime
    )

    jedis.set("shuffleMetrics", JSON.toJSONString(shuffleMap))

    //####################input   output#######
    val inputMetrics = metrics.inputMetrics
    val outputMetrics = metrics.outputMetrics
    val input_output = scala.collection.mutable.HashMap(
      "bytesRead" -> inputMetrics.bytesRead, //读取的大小
      "recordsRead" -> inputMetrics.recordsRead, //总记录数
      "bytesWritten" -> outputMetrics.bytesWritten,
      "recordsWritten" -> outputMetrics.recordsWritten
    )

    jedis.set("inputOutputMetrics", JSON.toJSONString(input_output))

    //####################taskInfo#######
    val taskInfo = taskEnd.taskInfo

    val taskInfoMap = scala.collection.mutable.HashMap(
      "taskId" -> taskInfo.taskId,
      "host" -> taskInfo.host,
      "speculative" -> taskInfo.speculative, //推测执行
      "failed" -> taskInfo.failed,
      "killed" -> taskInfo.killed,
      "running" -> taskInfo.running
    )

    jedis.set("taskInfo", JSON.toJSONString(taskInfoMap))

    JedisUtil.returnJedis(jedis)

  }
}
