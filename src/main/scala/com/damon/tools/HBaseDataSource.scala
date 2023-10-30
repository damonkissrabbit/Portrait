package com.damon.tools

import com.damon.bean.HBaseMeta
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}


class HBaseDataSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister with Serializable {

  /**
   * 读取数据源
   * @param sqlContext
   * @param parameters 在调用当前DataSource的时候传入的option键值对
   * @return
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val meta: HBaseMeta = parseMeta(parameters)
    new HBaseReadableRelation(sqlContext, meta)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val meta: HBaseMeta = parseMeta(parameters)
    val relation: HBaseWritableRelation = new HBaseWritableRelation(sqlContext, meta, data)
  }

  override def shortName(): String = "hbase"

  def parseMeta(params: Map[String, String]): HBaseMeta = {
    HBaseMeta(
      params.getOrElse(HBaseMeta.INTYPE, ""),
      params.getOrElse(HBaseMeta.ZKHOSTS, ""),
      params.getOrElse(HBaseMeta.ZKPORT, ""),
      params.getOrElse(HBaseMeta.HBASETABLE, ""),
      params.getOrElse(HBaseMeta.FAMILY, ""),
      params.getOrElse(HBaseMeta.SELECTFIELDS, ""),
      params.getOrElse(HBaseMeta.ROWKEY, "")
    )
  }
}
