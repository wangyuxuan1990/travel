package com.travel.programApp

import java.util

import com.travel.common.District
import com.travel.utils.{HbaseTools, SparkUtils}
import com.uber.h3core.H3Core
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.jts.geom.{GeometryFactory, Polygon}
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable

/**
 * @author wangyuxuan
 * @date 2020/3/9 16:49
 * @description 实现虚拟车站的功能
 */
object SparkSQLVirtualStation {
  def main(args: Array[String]): Unit = {
    // 第一步：读取hbase当中存储的海口市的数据
    val conf: SparkConf = new SparkConf().setAppName("SparkSQLVirtualStation").setMaster("local[1]")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "node01,node02,node03")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.setInt("hbase.client.operation.timeout", 3000)
    // 读取hbase的数据，获取到了df
    val hbaseFrame: DataFrame = HbaseTools.loadHBaseData(sparkSession, hconf)
    hbaseFrame.createOrReplaceTempView("order_df")
    // 第二步：计算所有的虚拟车站
    // 计算我们的虚拟车站，将每一个经纬度，转换成为hashCode码值
    val h3: H3Core = H3Core.newInstance()
    // 自定sql函数
    sparkSession.udf.register("locationToH3", new UDF3[String, String, Int, Long] {
      override def call(t1: String, t2: String, t3: Int): Long = h3.geoToH3(t1.toDouble, t2.toDouble, t3)
    }, DataTypes.LongType)
    val order_sql = "select order_id, city_id, starting_lng, starting_lat, locationToH3(starting_lat, starting_lng, 12) as h3code from order_df"
    val frame: DataFrame = sparkSession.sql(order_sql)
    frame.createOrReplaceTempView("order_grid")
    // 使用每个虚拟车站里面最小的一个经纬度，代表虚拟车站这个点
    val sql: String =
      s"""
         |select
         |order_id,
         |city_id,
         |starting_lng,
         |starting_lat,
         |row_number() over(partition by order_grid.h3code order by starting_lng, starting_lat asc) rn
         | from order_grid join (
         | select h3code, count(1) as totalResult from order_grid group by h3code having totalResult >= 1
         | ) groupcount on order_grid.h3code = groupcount.h3code
         |having(rn=1)
      """.stripMargin
    // 上面的sql语句，将每个经纬度转换成为了一个HashCode码值，然后对hashCode码值分组，获取每个组里面经纬度最小的那一个，得到这个经纬度，然后再计算，这个经纬度坐落在哪一个区里面
    val virtual_frame: DataFrame = sparkSession.sql(sql)
    // 判断每个虚拟车站一个经纬度，这个经纬度坐落在哪一个区里面，就能知道每个区里面有多少个虚拟车站了
    val virtual_rdd: RDD[Row] = virtual_frame.rdd

    //    //计算出来了所有的虚拟车站
    //    val virtual_rdd: RDD[Row] = SparkUtils.getVirtualFrame(sparkSession)

    // 第三步：确定海口市每个区的边界
    // 计算出每个区域边界 并且将边界进行广播
    val districtBroadCast: Broadcast[util.ArrayList[District]] = SparkUtils.broadCastDistrictValue(sparkSession)
    // 第四步：判断虚拟车站属于哪一个区，接口
    // 计算虚拟车站究竟再哪一个区里面
    virtual_rdd.mapPartitions(eachPartition => {
      // 使用JTS-Tools来通过多个经纬度，画出多边形
      val geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory(null)
      val reader: WKTReader = new WKTReader(geometryFactory)
      // 将哪一个区的，哪一个边界求出来
      // 将我们每一个区的经纬度的点连接起来，成为一个形状
      val wktPolygons: mutable.Buffer[(District, Polygon)] = SparkUtils.changeDistictToPolygon(districtBroadCast, reader)

      // 获取到了每一个虚拟车站这个数据
      eachPartition.map(row => {

      })
    })
  }
}
