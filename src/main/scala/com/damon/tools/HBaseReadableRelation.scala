package com.damon.tools

import com.damon.bean.HBaseMeta
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
 * 真正读取HBase数据源的Relation
 */
class HBaseReadableRelation(context: SQLContext, meta: HBaseMeta) extends BaseRelation with TableScan with Serializable {
  // 定义sqlContext
  override def sqlContext: SQLContext = context

  // 定义数据结构schema，里面定义列名/列的类型/当前列是否为空
  // 定义数据源中数据的结构
  override def schema: StructType = {
    // 构建一个StructType，所有列的元数据信息(id:name,type,type  rule:name,type,false)
    // meta.selectFields => 从Mysql的4级标签获取到的selectFields -> id,job
    val fields: Array[StructField] = meta.selectFields.split(",")
      .map(fieldName => {
        StructField(fieldName, StringType, nullable = true)
      })
    // 使用fields构建StructType
    StructType(fields)
  }

  // 定义了如何从数据源中读取数据，方法返回值表示从数据源中读取的数据

  /**
   *
   * @return 构建数据源，我们可以自己定义从HBase中拿到的额数据，封装为Row返回
   */
  override def buildScan(): RDD[Row] = {
    // 数据在哪？HBase
    // 返回的数据类型为RDD[Row]
    // 定义Hbase相关的conf
    val conf: Configuration = new Configuration()
    conf.set("hbase.zookeeper.property.clientPort", meta.zkPort)
    conf.set("hbase.zookeeper.quorum", meta.zkHosts)
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFormat.INPUT_TABLE, meta.hbaseTable)

    // 从hadoop中构建我们的数据源RDD
    // ImmutableBytesWritable类的作用是提供一个不可变的、可序列化的字节数组包装器，方便在Hadoop和Hbase中进行数据的传递和处理
    val sourceRDD: RDD[(ImmutableBytesWritable, Result)] = context.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    // 获取Result数据
    val resultRDD: RDD[Result] = sourceRDD.map(_._2)
    // 从Result中获取我们需要的字段，别的字段不要
    // map == result => row
    resultRDD.map(result => {
      // 获取列的名字，我们可以使用selectFields进行切割
      val seq: Seq[String] = meta.selectFields.split(",")
        // 将列名转换为row，一行数据
        // 将String列名 => 具体列的值
        .map(fieldName => {
          // 如果使用result获取数据，数据默认类型为byte数组，需要使用Hbase的工具类Bytes将数据转化为String
          Bytes.toString(result.getValue(meta.family.getBytes, fieldName.getBytes))
        }).toSeq
      // 将列的值封装为row
      Row.fromSeq(seq)
    })
  }
}
