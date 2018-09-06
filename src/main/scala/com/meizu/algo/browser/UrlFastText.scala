package com.meizu.algo.browser

import classify.ClassifyDataFrame
import com.meizu.algo.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by dxp 2018-05-04
  */
object UrlFastText {
  val business = UrlConfig.business

  val dataTable = UrlConfig.dataTable

  val titleSegTable = UrlConfig.titleSegTable
  val contentSegTable = UrlConfig.contentSegTable

  val titlePcatTable = UrlConfig.titlePcatTable
  val contentPcatTable = UrlConfig.contentPcatTable
  val titleCcatTable = UrlConfig.titleCcatTable
  val contentCcatTable = UrlConfig.contentCcatTable

  val unionCatTable = UrlConfig.unionCattable
  var isDebug = UrlConfig.isDebug

  /*
  val subCats = Array("两性情感","体育","健康","军事","历史",
    "娱乐","房产","教育","旅游","时尚","星座",
    "汽车","游戏","社会","科学探索","科技","育儿","财经",
    "国际")
    */
  val subCats = Array("两性情感","体育","健康","军事","历史",
    "娱乐","房产","教育","旅游","时尚","星座",
    "汽车","游戏","社会","科学探索","科技","育儿","财经",
    "动漫","文化","时政")

  def main(args: Array[String]): Unit = {
    val op: String = args(0)
    isDebug = args(1).toLowerCase == "true"
    val statDate = args(2)
    op match {
      case "querydata" => queryData(statDate)
      case "seg" => segData(statDate,titleSegTable, contentSegTable)
      case "titlep" => {
        catParent(statDate,titleSegTable, titlePcatTable)
      }
      case "titlec" => catChild(statDate,titlePcatTable,titleCcatTable)
      case "contentp" =>{
        catParent(statDate,contentSegTable, contentPcatTable)
      }
      case "contentc" => catChild(statDate,contentPcatTable, contentCcatTable)
      case "union" => {
        unionCat(statDate, unionCatTable)
      }

      case _ => {
        println(
          """
            |Usage: [cat|catprob|catdis]
          """.stripMargin)
        sys.exit(1)
      }
    }
  }


  /*
  统计每天用户浏览文章及次数
   */
  def queryData(statDate: String) = {
    val sparkEnv = new SparkEnv("queryData").sparkEnv
    // hash id 针对文章内容不为空进行。因而需要把内容为空而标题不为空的文章合并出来
    val ori_sql =
      s"""
        |select fid,ftitle,fcontent from uxip.dwd_browser_url_creeper where stat_date=${statDate} and
        |src_type in
        |("com.ss.android.article.news", "com.android.browser","com.ifeng.news2","com.netease.newsreader.activity","com.tencent.news")
      """.stripMargin

    val data = sparkEnv.sql(ori_sql).rdd.map(r=>{
      (r.getAs[Long](0),r.getAs[String](1),r.getAs[String](2))
    })
    val cols = Array("fid", "ftitle","fcontent")
    val colsType = Array("long", "string","string")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._2,r._3)
    }), st)
    DXPUtils.saveDataFrame(df, dataTable, statDate, sparkEnv)
  }

  /*
  对每天用户浏览的非uc文章进行分词
   */
  def segData(statDate: String, titleSegTable:String, contentSegTable:String) = {
    val sparkEnv = new SparkEnv("seg_data").sparkEnv

    val ori_sql =
      s"""select fid,ftitle,fcontent from ${UrlConfig.dataTable} where stat_date=${statDate}
      """.stripMargin

    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
    val data = sparkEnv.sql(sql).rdd.repartition(200).mapPartitions(it => {
      it.map(r => {
        val fid = r.getLong(0)
        val ftitle = r.getAs[String](1)
        val fcontent = r.getAs[String](2)
        val titleStr = if (ftitle == null){
          ""
        } else {
          ftitle.trim.replaceAll("\n"," ").replaceAll("<.*?>", " ")
        }
        val title = Segment.segment2(titleStr)

        val contentStr = if (fcontent == null) {
          ""
        } else {
          fcontent.trim.replaceAll("\n", " ").replaceAll("<.*?>", " ")
        }
        val content = Segment.segment2(contentStr)
        (fid, title, content)
      })
    })
    data.cache()

    val cols = Array("fid", "words")
    val colsType = Array("long", "string")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val titleDF: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._2.mkString(" "))
    }), st)
    DXPUtils.saveDataFrame(titleDF, titleSegTable, statDate, sparkEnv)

    val contentDF: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row(r._1, r._3.mkString(" "))
    }), st)
    DXPUtils.saveDataFrame(contentDF, contentSegTable, statDate, sparkEnv)
  }


  /*
  根据cat查询数据，结果存在分区表，分区有两个字段stat_date, cat
   */
  def catParent(statDate: String, inputTable:String, outputTable:String) = {
    val cat = "all"
    val sparkEnv = new SparkEnv(s"cat ${cat}").sparkEnv
    val ori_sql = s"select fid,words from ${inputTable} where stat_date=${statDate} "
    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql

    val catData = ClassifyDataFrame.classifyData(sparkEnv.sql(sql),cat)

    val cols = Array("fid", "words", "cat", "prob")
    val colsType = Array("long", "string", "string", "double")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(catData.map(r => {
      Row(r._1, r._2, r._3, r._4)
    }), st).repartition(200)
    DXPUtils.saveDataFrame(df, outputTable, statDate, sparkEnv)
  }

  def catChild(statDate: String, inputTable:String, outputTable:String) = {
    val sparkEnv = new SparkEnv(s"cat child").sparkEnv

    val cat = subCats(0)
    val ori_sql = s"select fid,words from ${inputTable} where stat_date=${statDate} and cat='${cat}'"
    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
    var catData:RDD[(Long,String,String,Double)] = ClassifyDataFrame.classifyData(sparkEnv.sql(sql),cat)

    for(cat <- subCats.slice(1,subCats.length)){
      val ori_sql = s"select fid,words from ${inputTable} where stat_date=${statDate} and cat='${cat}'"
      val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
      catData = catData.union(ClassifyDataFrame.classifyData(sparkEnv.sql(sql),cat))
    }

    val cols = Array("fid", "words", "cat", "prob")
    val colsType = Array("long", "string", "string", "double")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(catData.map(r => {
      Row(r._1, r._2, r._3, r._4)
    }), st).repartition(200)

    DXPUtils.saveDataFrame(df, outputTable, statDate, sparkEnv)
  }

  def unionCat(statDate:String,outputTable:String): Unit ={
    val sparkEnv = new SparkEnv(s"cat child").sparkEnv
    val ori_sql =
      s"""
         | select a.fid as fid,
         | a.cat as title_pcat,a.prob as title_pprob,
         | b.cat as title_ccat,b.prob as title_cprob,
         | c.cat as content_pcat,c.prob as content_pprob,
         | d.cat as content_ccat,d.prob as content_cprob from
         | (select fid,cat,prob from ${UrlConfig.titlePcatTable} where stat_date=${statDate}) a
         | left outer join
         | (select fid,cat,prob from ${UrlConfig.titleCcatTable} where stat_date=${statDate}) b
         | on a.fid = b.fid
         | left outer join
         | (select fid,cat,prob from ${UrlConfig.contentPcatTable} where stat_date=${statDate}) c
         | on a.fid = c.fid
         | left outer join
         | (select fid,cat,prob from ${UrlConfig.contentCcatTable} where stat_date=${statDate}) d
         | on a.fid = d.fid
       """.stripMargin

    val sql = if (isDebug) ori_sql + " limit 1000" else ori_sql
    val df = sparkEnv.sql(sql)
    DXPUtils.saveDataFrame(df, outputTable, statDate, sparkEnv)
  }
}

