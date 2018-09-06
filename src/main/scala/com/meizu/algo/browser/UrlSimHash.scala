package com.meizu.algo.browser

import com.meizu.algo.util.{DXPUtils, FNVHash, Segment, SparkEnv}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer
import util._

/*
dxp 2018-05-11
从hive表查出id,content这样的字段,id,content均为string类型
对content进行分词,关键词提取,hash,及聚合

dxp 2018-08-30
对指定的src_type进行处理，去掉电商类src_type
 */
object UrlSimHash {
  var isDebug: Boolean = UrlConfig.isDebug
  val business = UrlConfig.business
  val segTable = UrlConfig.segTable
  val hashTable = UrlConfig.hashTable
  val sameTable = UrlConfig.sameTable
  val distanceTable = UrlConfig.distanceTable
  val dupTable = UrlConfig.dupTable
  // 根据op不同，执行不同的操作
  def main(args: Array[String]): Unit = {
    val op: String = args(0)
    isDebug = args(1).toLowerCase == "true"
    val statDate = args(2)

    op match {
      case "seg" => queryAndSegData(statDate)
      case "hash" => makeHash(statDate)
      case "same" => findSame(statDate)
      case "kbits"=>{
        //val kBits = args(3).toInt
        //val bId = args(4).toInt
        findkBitsDistance(statDate, 1, 0)
        findkBitsDistance(statDate, 1, 1)
        findkBitsDistance(statDate, 1, 2)
        findkBitsDistance(statDate, 1, 3)

      }
      case "savedup" =>{
        //val thres = args(3).toInt
        saveDup(statDate, 10)
      }
      case _ => {
        println(
          """
            |Usage: [seg|keyword|hash]
          """.stripMargin)
        sys.exit(1)
      }
    }
  }

  /*
   查询两个字段fid(Long), fcontent(String)
   并进行分词，不对分词结果进行过滤。这里分词后,
   不同的算法有不同的优化处理。比如fasttext,去掉标点就行
   如simhash, 则需要去掉某些词性的词.
    */
  def queryAndSegData(statDate: String) = {
    val sparkEnv = new SparkEnv("seg").sparkEnv

    val ori_sql =
      s"""
        |select fid,fcontent from uxip.dwd_browser_url_creeper where stat_date=${statDate} and
        |src_type in
        |("com.ss.android.article.news", "com.android.browser","com.ifeng.news2","com.netease.newsreader.activity","com.tencent.news")
      """.stripMargin

    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
    val data = sparkEnv.sql(sql).rdd.map(r => {
        val fid = r.getLong(0)
        //只拿出汉字
        val contentStr = if (r.getString(1) == null) "" else
          r.getAs[String](1).replaceAll("[^\u4e00-\u9FCB]+", " ")

        (fid, Segment.segmentWithNature(contentStr).filter(w => {
          // 去掉以cdempqruwxyz开头的词性
          // 2018-08-31 修改为只取名词且词长大于1
          'n' == w.nature.firstChar() && w.word.length > 1
        }).map(_.word).mkString(","))
      })

    val cols = Array("fid", "words")
    val colsType = Array("long", "string")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._2)
    }), st).repartition(200)
    DXPUtils.saveDataFrame(df, segTable, statDate, sparkEnv)
  }

  // 计算每篇文章的hash值
  def makeHash(statDate: String) = {
    val sparkEnv = new SparkEnv("hash").sparkEnv

    val ori_sql = "select fid,words " +
      s"from ${segTable} where stat_date=${statDate}"

    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
    val data = sparkEnv.sql(sql).rdd.map(r => {
      if(r.getAs[String](1) == ""){
        (r.getAs[Long](0), 0L)
      }else
      (r.getAs[Long](0), FNVHash.hashContentStr(r.getAs[String](1)).longValue())
    })

    val cols = Array("fid", "hash")
    val colsType = Array("long", "long")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._2)
    }), st)
    DXPUtils.saveDataFrame(df, hashTable, statDate, sparkEnv)
  }

  // 找出完全相同的hash值.
  def findSame(statDate: String) = {
    val sparkEnv = new SparkEnv("hash").sparkEnv

    val ori_sql = "select fid,hash " +
      s"from ${hashTable} where stat_date=${statDate}"

    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
    val data = sparkEnv.sql(sql).rdd.map(r => {
      (r.getAs[Long](1), Array(r.getAs[Long](0)))
    }).filter(_._2 != 0).reduceByKey(_ ++ _).map(r => {
      (r._1, r._2(0), r._2.mkString(","), r._2.length)
    })

    val cols = Array("hash", "fid", "samefid", "samelen")
    val colsType = Array("long", "long", "string", "int")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._2, r._3, r._4)
    }), st)
    DXPUtils.saveDataFrame(df, sameTable, statDate, sparkEnv)
  }

  /*
  k个bit不同
  将hash切成四块，每块16bits. 在保证任意三块相同的情况下进行聚合，再计算haming-distance
  总共会进行四次去重
   */
  def findkBitsDistance(statDate: String, kBits: Int, bId: Int) = {

    //将hash分成四个块，每次取三个块相同
    def moveByte(hash: Long, bId: Int) = {
      bId match {
        case 0 => (hash & 0x0000ffffffffffffL)
        case 1 => (hash & 0xffff0000ffffffffL)
        case 2 => (hash & 0xffffffff0000ffffL)
        case 3 => (hash & 0xffffffffffff0000L)
        case _ => {
          println("bad bId, exit...")
          sys.exit(1)
        }
      }
    }

    //处理输入表与输出表
    val kBitsTable = s"${distanceTable}_${kBits}"
    val (inputTable,outputTable) = bId match {
      case 0 => (sameTable,s"${kBitsTable}_0")
      case 1|2|3 =>{
        (s"${kBitsTable}_${bId-1}", s"${kBitsTable}_${bId}")
      }
      case _ =>{
        println("bad bId, exit...")
        sys.exit(1)
      }
    }

    val sparkEnv = new SparkEnv("kbits").sparkEnv
    val ori_sql: String = "select fid,hash,samelen " +
      s"from ${inputTable} where stat_date=${statDate}"

    val sql: String = if (isDebug) ori_sql + " limit 10000" else ori_sql
    val data = sparkEnv.sql(sql).rdd.map(r => {
      (moveByte(r.getAs[Long](1), bId), Array((r.getAs[Long](0), r.getAs[Long](1),r.getAs[Int](2))))
    }).reduceByKey(_ ++ _).flatMap(r => {
      val arrayBuffer = new ArrayBuffer[(Long, Long, String, Int)]()
      // 每次取前一次中重复次数最高的fid
      var sameArr = r._2.sortWith(_._3 > _._3)
      while (sameArr.length > 0) {
        val w = sameArr(0)
        val wArr = sameArr.filter(m => {
          w._3 >= m._3 && FNVHash.distance(w._2, m._2, kBits)
        })
        //至少含有自身，所以wArr不为空
        arrayBuffer.append((w._1, w._2, wArr.map(_._1).take(50000).mkString(","), wArr.map(_._3).sum))
        sameArr = sameArr.filterNot(s => wArr.contains(s))
      }
      arrayBuffer
    })

    val cols = Array("fid", "hash", "samefid", "samelen")
    val colsType = Array("long", "long", "string", "int")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._2, r._3, r._4)
    }), st)
    DXPUtils.saveDataFrame(df, outputTable, statDate, sparkEnv)
  }

  // 找出重复量大于thres的数据,且找到fid对应的category, 以便业务核查
  def saveDup(statDate:String, thres:Int) = {
    val sparkEnv = new SparkEnv("kbits").sparkEnv
    val inputTable = s"${distanceTable}_1_3"
    val outputTable = dupTable
    val ori_sql: String =
      s"""
        | select a.fid, a.hash, a.samelen,b.furl,
        | b.ftitle,b.fkeywords,b.fcontent,b.fcontent_len,b.src_type from
        | (select fid, hash, samelen from ${inputTable} where stat_date=${statDate}
        | and samelen >= ${thres}) a
        | join (select regexp_extract(furl,"(http[:s/]+[a-z.]+/)",1) as furl,fid,ftitle,
        | fkeywords,fcontent,length(fcontent) as fcontent_len,src_type from
        | uxip.dwd_browser_url_creeper where stat_date=${statDate}) b
        | on a.fid = b.fid
      """.stripMargin

    val sql: String = if (isDebug) ori_sql + " limit 1000" else ori_sql
    val dataRdd = sparkEnv.sql(sql).rdd.filter(r=>{
      //去掉不可用数据
      try{
        val furl =  r.getAs[String]("furl")
        val ftitle = r.getAs[String]("ftitle")
        val fcontent = r.getAs[String]("fcontent")
        (UrlConfig.useless_url_arr ++ UrlConfig.ec_url_arr).filter(url=>{
          furl.indexOf(url) != -1
        }).length == 0 && {
          ftitle !=null && ftitle.trim.indexOf("百度一下") == -1
        } && {
          fcontent != null && !fcontent.startsWith("ERROR")
        }
      }catch{
        case _:Exception => true
      }
    })
    val st = sparkEnv.sql(sql).schema
    val df = sparkEnv.createDataFrame(dataRdd,st)
    //toDF("fid","hash","samelen","furl", "ftitle","fkeywords","fcontent","fcontent_len","src_type")
    DXPUtils.saveDataFrame(df,outputTable,statDate,sparkEnv)
  }
}
