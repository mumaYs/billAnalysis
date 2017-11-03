package com.ccfsoft.bigdata.billAnalysis.spark.graphX

import com.ccfsoft.bigdata.utils.{PropertyConstants, TimeUtil}
import org.apache.spark.graphx.{VertexRDD, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * 话单关系图计算
  */
object BillGraphX {

  def billGraphX(spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    // 所有的顶点
    val nodesDf = spark.sql("select distinct(*) from (select T1.own_phone from BILL T1 union all select T2.other_phone from BILL T2) T")
    val nodes = nodesDf.collect().map(x => (x.getLong(0), x.getLong(0).toString))
    val nodesRDD: RDD[(VertexId, String)] = sc.parallelize(nodes)

    // 1.分析强联系人
    val relationDf = spark.sql("select T.own_phone,T.other_phone,T.talk_time,T.begin_date from BILL T")
    val relationships = relationDf.collect().map(x => Edge(x.getLong(0), x.getLong(1), x.getString(2).toInt))
    val relationRDD: RDD[Edge[PartitionID]] = sc.parallelize(relationships)
    // 绘图
    val tiesGraph = Graph(nodesRDD, relationRDD)
    val tiesPerson: VertexRDD[Map[String,PartitionID]] = tiesGraph.aggregateMessages[Map[String,PartitionID]](
      triplet => {
        triplet.sendToSrc(Map(triplet.dstAttr -> triplet.attr))
      },
      (a, b) => {
        if(a.keySet.companion(b.keySet.max) != null){
          if(a.get(b.keySet.max) != None){
            a.updated(b.keySet.max,a.get(b.keySet.max).get + b.values.max)
          }else{
            a.++(b)
          }
        }else{
          a.++(b)
        }
      }
    )

    val rowList = new ListBuffer[Row]()
    for(p1 <- tiesPerson.collect()){
      for((x,y) <- p1._2){
        //一周通话时间大于两个小时，则为强关系
        if(y > 7200)
        rowList.append(Row(p1._1,x.toLong,1,y,TimeUtil.getLocalDataTime))
      }
    }
    dbWrite(spark, rowList)

    // 2.分析工作关系人
    val workRelationDf = spark.sql("select T.own_phone,T.other_phone from BILL T WHERE T.`begin_time` BETWEEN '08:00:00' " +
      "AND '12:00:00' OR T.`begin_time` BETWEEN '13:30:00'  AND '18:00:00'")
    val workRelationships = workRelationDf.collect().map(x => Edge(x.getLong(0), x.getLong(1), 1))
    val workRelationRDD: RDD[Edge[PartitionID]] = sc.parallelize(workRelationships)
    // 绘图分析
    val workGraph = Graph(nodesRDD, workRelationRDD)
    val workRelation: VertexRDD[Map[String,PartitionID]] = workGraph.aggregateMessages[Map[String,PartitionID]](
          triplet => {
            triplet.sendToSrc(Map(triplet.dstAttr -> triplet.attr))
          },
          (a, b) => {
            if(a.keySet.companion(b.keySet.max) != null){
              if(a.get(b.keySet.max) != None){
                a.updated(b.keySet.max,a.get(b.keySet.max).get + b.values.max)
              }else{
                a.++(b)
              }
            }else{
              a.++(b)
            }
          }
        )

    //转换
    val workRowList = new ListBuffer[Row]()
    for(p1 <- workRelation.collect()){
      for((x,y) <- p1._2){
        //一个月内工作时间通话次数大于25次，则为工作关系
        if(y > 25)
          workRowList.append(Row(p1._1,x.toLong,3,y,TimeUtil.getLocalDataTime))
      }
    }
    dbWrite(spark, workRowList)
  }

  private def dbWrite(spark: SparkSession, rowList: ListBuffer[Row]) = {
    // DataFrame的schema结构
    val schema =
      StructType(
          StructField("BILL_ID", LongType, false) ::
          StructField("CONTACT_BILL_ID", LongType, false) ::
          StructField("CONTACT_TYPE", IntegerType, false) ::
          StructField("RATE", IntegerType, false) ::
          StructField("CREATE_TIME", StringType, false) :: Nil)


    // 生成DataFrame
    val dataFrame = spark.createDataFrame(rowList.toList, schema)

    //将结果写入数据库中
    val properties = PropertyConstants.getProperties;

    dataFrame.write
      .mode("append")
      .jdbc(properties.getProperty("url"), "BILL_CONTACTS", properties)
  }
}
