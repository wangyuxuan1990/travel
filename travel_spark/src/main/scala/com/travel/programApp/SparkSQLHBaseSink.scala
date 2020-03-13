package com.travel.programApp

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * @author wangyuxuan
 * @date 2020/3/13 11:16
 * @description 自定义SparkSQL数据源，将数据保存到Hbase
 */
object SparkSQLHBaseSink {
  def saveToHBase(dataFrame: DataFrame, tableName: String, rowkey: String, fields: String): Unit = {
    dataFrame.write
      .format("com.travel.programApp.hbaseSink.HBaseSink")
      .mode(SaveMode.Overwrite)
      .option("hbase.table.name", tableName)
      .option("hbase.rowkey", rowkey)
      .option("hbase.fields", fields)
      .save()
  }
}
