package com.travel.spark

import com.travel.bean.{DriverInfo, Opt_alliance_business, OrderInfo, RegisterUsers}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author wangyuxuan
 * @date 2020/3/14 9:40 下午
 * @description sparkConf参数设置
 *              线上的数据量，每天加上轨迹数据在300-500GB左右，使用了三十多台服务器来做支撑
 */
object SparkEngine {
  def getSparkConf: SparkConf = {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.driver.cores", "4") //设置driver的CPU核数  使用哪种模式进行部署：使用 on yarn模式来进行部署   cluster  还是client模式
      .set("spark.driver.maxResultSize", "2g") //设置driver端结果存放的最大容量，这里设置成为2G，超过2G的数据,job就直接放弃，不运行了
      .set("spark.driver.memory", "4g") //driver给的内存大小
      .set("spark.executor.memory", "8g") // 每个executor的内存
      .set("spark.submit.deployMode", "cluster") //spark 任务提交模式，线上使用cluster模式，开发使用client模式
      .set("spark.worker.timeout", "500") //基于standAlone模式下提交任务，worker的连接超时时间
      .set("spark.cores.max", "10") //基于standAlone和mesos模式下部署，最大的CPU和数量
      .set("spark.rpc.askTimeout", "600s") //spark任务通过rpc拉取数据的超时时间
      .set("spark.locality.wait", "5s") //每个task获取本地数据的等待时间，默认3s钟，如果没获取到，依次获取本进程，本机，本机架数据
      .set("spark.task.maxFailures", "5") //允许最大失败任务数，根据自身容错情况来定
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //配置序列化方式
      .set("spark.streaming.kafka.maxRatePerPartition", "5000") //使用directStream方式消费kafka当中的数据，获取每个分区数据最大速率
      .set("spark.streaming.backpressure.enabled", "true") //开启sparkStreaming背压机制，接收数据的速度与消费数据的速度实现平衡
      //  .set("spark.streaming.backpressure.pid.minRate","10")
      .set("spark.driver.host", "localhost") //配置driver地址
      //shuffle相关参数调优开始
      .set("spark.reducer.maxSizeInFlight", "96m") //reduceTask拉取map端输出的最大数据量，调整太大有OOM的风险
      .set("spark.shuffle.compress", "true") //开启shuffle数据压缩
      .set("spark.default.parallelism", "10") //设置任务的并行度
      .set("spark.files.fetchTimeout", "120s") //设置文件获取的超时时间
      //网络相关参数
      .set("spark.rpc.message.maxSize", "256") //RPC拉取数据的最大数据量，单位M
      .set("spark.network.timeout", "120s") //网络超时时间设置
      .set("spark.scheduler.mode", "FAIR") //spark 任务调度模式  使用 fair公平调度
      //spark任务资源动态划分  https://spark.apache.org/docs/2.3.0/job-scheduling.html#configuration-and-setup
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.shuffle.service.enabled", "true")
      .set("spark.dynamicAllocation.executorIdleTimeout", "120s") //executor空闲时间超过这个值，该executor就会被回收
      .set("spark.dynamicAllocation.minExecutors", "0") //最少的executor个数
      .set("spark.dynamicAllocation.maxExecutors", "32") //最大的executor个数  根据自己实际情况调整
      .set("spark.dynamicAllocation.initialExecutors", "4") //初始executor个数
      .set("spark.dynamicAllocation.schedulerBacklogTimeout", "5s") //pending 状态的task时间，过了这个时间继续pending ，申请新的executor
      .setMaster("local[1]")
      .setAppName("Stream")
    sparkConf.set("spark.speculation", "true") //开启推测执行
    sparkConf.set("spark.speculation.interval", "100s") // 每隔多久检测一次是否需要进行推测执行任务
    sparkConf.set("spark.speculation.quantile", "0.9") //完成任务的百分比，然后才能启动推测执行
    sparkConf.set("spark.streaming.backpressure.initialRate", "500") //开启sparkStreaming的背压机制，然后第一批次获取数据的最大速率

    sparkConf.registerKryoClasses(
      Array(
        classOf[OrderInfo],
        classOf[Opt_alliance_business],
        classOf[DriverInfo],
        classOf[RegisterUsers]
      )
    )
    sparkConf
  }

  def getSparkSession(sparkConf: SparkConf): SparkSession = {
    val sparkSession: SparkSession = SparkSession.builder()
      .config(sparkConf)
      //调度模式
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.executor.memoryOverhead", "512") //堆外内存
      //  .config("enableSendEmailOnTaskFail", "true")
      //   .enableHiveSupport() //开启支持hive
      .getOrCreate()
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "n1")
    sparkSession
  }
}
