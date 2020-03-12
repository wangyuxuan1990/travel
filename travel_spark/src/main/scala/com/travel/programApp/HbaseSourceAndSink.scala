package com.travel.programApp

import java.util
import java.util.Optional

import com.travel.utils.HbaseTools
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * @author wangyuxuan
 * @date 2020/3/12 11:48
 * @description sparkSQL自定义数据源
 */
object HbaseSourceAndSink {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("HbaseSourceAndSink").master("local[2]").getOrCreate()
    // format需要我们自定义数据源
    val df: DataFrame = spark.read.format("com.travel.programApp.HBaseSource")
      .option("hbase.table.name", "spark_hbase_sql") // 我们自己带的一些参数
      .option("cf.cc", "cf:name,cf:score") // 定义我们查询hbase的哪些列
      .option("schema", "`name` STRING,`score` STRING") // 定义我们表的schema
      .load()

    df.createOrReplaceTempView("sparkHBaseSQL")
    df.printSchema()
    // 分析得到的结果数据，将结果数据，保存到hbase，redis或者mysql或者es等等都行
    val resultDF: DataFrame = spark.sql("select * from sparkHBaseSQL where score > 60")

    resultDF.write.format("com.travel.programApp.HBaseSource")
      .mode(SaveMode.Overwrite)
      .option("hbase.table.name", "spark_hbase_write")
      .option("cf", "cf")
      .save()
  }
}

/**
 * 可以自定义数据源，实现数据的查询
 */
class HBaseSource extends DataSourceV2 with ReadSupport with WriteSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val tableName: String = options.get("hbase.table.name").get()
    val cfAndCC: String = options.get("cf.cc").get()
    val schema: String = options.get("schema").get()
    new HBaseDataSourceReader(tableName, cfAndCC, schema)
  }

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {
    val tableName: String = options.get("hbase.table.name").get()
    val cf: String = options.get("cf").get()
    Optional.of(new HBaseDataSourceWriter(tableName, cf))
  }
}

class HBaseDataSourceWriter(tableName: String, cf: String) extends DataSourceWriter {
  /**
   * 将我们的数据保存起来，全部依靠这个方法
   *
   * @return
   */
  override def createWriterFactory(): DataWriterFactory[Row] = {
    new HBaseDataWriterFactory(tableName, cf)
  }

  /**
   * 提交数据时候带的一些注释信息
   *
   * @param messages
   */
  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  /**
   * 数据插入失败的时候带的一些注释信息
   *
   * @param messages
   */
  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}

class HBaseDataWriterFactory(tableName: String, cf: String) extends DataWriterFactory[Row] {
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new HBaseDataWriter(tableName, cf)
  }
}

class HBaseDataWriter(tableName: String, cf: String) extends DataWriter[Row] {
  private val conn: Connection = HbaseTools.getHbaseConn
  private val table: Table = conn.getTable(TableName.valueOf(tableName))
  private var i: Int = 1

  /**
   * 写入数据
   *
   * @param record
   */
  override def write(record: Row): Unit = {
    val rowKey: String = StringUtils.leftPad(i + "", 4, "0")
    val put: Put = new Put(rowKey.getBytes())
    for (field <- record.schema.fieldNames) {
      val value: String = record.getString(record.fieldIndex(field))
      put.addColumn(cf.getBytes(), field.getBytes(), value.getBytes())
    }
    table.put(put)
    i += 1
  }

  /**
   * 数据的提交方法，数据插入完成之后，在这个方法里面进行数据的事务提交
   *
   * @return
   */
  override def commit(): WriterCommitMessage = {
    table.close()
    conn.close()
    null
  }

  override def abort(): Unit = {

  }
}

class HBaseDataSourceReader(tableName: String, cfAndCC: String, schema: String) extends DataSourceReader {
  private val structType: StructType = StructType.fromDDL(schema)

  /**
   * 定义我们映射的表的schema
   *
   * @return
   */
  override def readSchema(): StructType = {
    structType
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    import scala.collection.JavaConverters._
    Seq(new HBaseReaderFactory(tableName, cfAndCC).asInstanceOf[DataReaderFactory[Row]]).asJava
  }
}

class HBaseReaderFactory(tableName: String, cfAndCC: String) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = {
    new HBaseReader(tableName, cfAndCC)
  }
}

/**
 * 自定义HBaseReader 实现了DataReader接口
 *
 * @param tableName
 * @param cfAndCC
 */
class HBaseReader(tableName: String, cfAndCC: String) extends DataReader[Row] {
  private var hbaseConnection: Connection = null
  private var table: Table = null
  private var resultScanner: ResultScanner = null

  // 获取我们hbase的数据就在这
  def getIterator: Iterator[Seq[AnyRef]] = {
    hbaseConnection = HbaseTools.getHbaseConn
    table = hbaseConnection.getTable(TableName.valueOf(tableName))
    resultScanner = table.getScanner(new Scan())
    // 获取到了每一条数据
    import scala.collection.JavaConverters._
    val iterator: Iterator[Seq[AnyRef]] = resultScanner.iterator().asScala.map(eachResult => {
      var list: ListBuffer[AnyRef] = ListBuffer[AnyRef]()
      val strings: Array[String] = cfAndCC.split(",")
      for (string <- strings) {
        val cfAndCcString: Array[String] = string.split(":")
        val cf: String = cfAndCcString(0)
        val cc: String = cfAndCcString(1)
        val value: String = Bytes.toString(eachResult.getValue(cf.getBytes(), cc.getBytes()))
        list += value
      }
      list.toList
    })
    iterator
  }

  // 最难想得到的
  val data: Iterator[Seq[AnyRef]] = getIterator

  /**
   * 这个方法反复不断的被调用，只要我们查询到了数据，就可以使用next方法一直获取下一条数据
   *
   * @return
   */
  override def next(): Boolean = {
    data.hasNext
  }

  /**
   * 获取到的数据在这个方法里面一条条的解析，解析之后，映射到我们提前定义的表里面去
   *
   * @return
   */
  override def get(): Row = {
    val seq: Seq[AnyRef] = data.next()
    Row.fromSeq(seq)
  }

  /**
   * 关闭一些资源
   */
  override def close(): Unit = {
    table.close()
    hbaseConnection.close()
  }
}
