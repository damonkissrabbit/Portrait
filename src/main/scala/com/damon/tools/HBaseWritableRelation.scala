package com.damon.tools

import com.damon.bean.HBaseMeta
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}


class HBaseWritableRelation(context: SQLContext, meta: HBaseMeta, data: DataFrame) extends BaseRelation with InsertableRelation with Serializable {
  override def sqlContext: SQLContext = context

  override def schema: StructType = data.schema

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val config: Configuration = new Configuration()
    config.set("hbase.zookeeper.property.clientPort", meta.zkPort)
    config.set("hbase.zookeeper.quorum", meta.zkHosts)
    config.set("zookeeper.znode.parent", "/hbase-unsecure")
    config.set("mapreduce.output.fileoutputformat.outputdir", "/test01")
    config.set(TableOutputFormat.OUTPUT_TABLE, meta.hbaseTable)

    val job: Job = Job.getInstance(config)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    data
      .rdd
      .coalesce(1)
      .map(row => {
        val rowKey: String = row.getAs("userId").toString
        val put = new Put(rowKey.getBytes())
        meta.selectFields.split(",")
          .map(fieldName => {
            val value: String = row.getAs(fieldName).toString
            put.addColumn(meta.family.getBytes, fieldName.getBytes, value.getBytes())
          })
        (new ImmutableBytesWritable(), put)
      })
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
