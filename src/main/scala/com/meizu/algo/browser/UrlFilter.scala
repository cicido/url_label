package com.meizu.algo.browser

import classify.ClassifyDataFrame
import com.meizu.algo.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.matching.Regex

/**
  * Created by dxp 2018-05-04
  */
object UrlFilter {
  val business = UrlConfig.business

  val unionAllCatTable = UrlConfig.unionDataTable
  val etlTable = UrlConfig.etlTable

  var isDebug = UrlConfig.isDebug

  def main(args: Array[String]): Unit = {
    val op: String = args(0)
    isDebug = args(1).toLowerCase == "true"
    val statDate = args(2)
    op match {
      case "union" => {
        unionArticle(statDate)
      }
      case "filter" => {
        filterArticle(statDate)
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

  def unionArticle(statDate: String) = {
    val sparkEnv = new SparkEnv("unionAllCat").sparkEnv
    val sql =
      s"""
         |select a.fid,a.furl,a.ftitle,a.fcontent_len,
         |a.src_type,b.hash,c.title_pcat, c.title_pprob, c.title_ccat, c.title_cprob,
         |c.content_pcat,c.content_pprob, c.content_ccat, c.content_cprob from
         |(select fid,regexp_extract(furl,"(http[:s/]+[^/]+/)",1) as furl,ftitle,
         |length(fcontent) as fcontent_len,src_type from uxip.dwd_browser_url_creeper where stat_date=${statDate} and
         |src_type in("com.ss.android.article.news", "com.android.browser",
         |"com.ifeng.news2","com.netease.newsreader.activity","com.tencent.news")) a
         |left outer join (select fid,hash from ${UrlConfig.hashTable} where stat_date=${statDate}) b
         |on a.fid = b.fid
         |join (select * from ${UrlConfig.unionCattable} where stat_date=${statDate}) c
         |on a.fid = c.fid
       """.stripMargin

    val df = sparkEnv.sql(sql)
    DXPUtils.saveDataFrame(df, unionAllCatTable, statDate, sparkEnv)
  }

  /*
  集中在最后一步进行规则过滤
  1. 数据绝对可用
  i. 标题与内容分类结果不等于unknow
    * 标题分类与内容分类结果相同,直接采用
    * furl判断只能用标题,采用标题分类结果,且父类概率大于0.5
    * furl能判断出父类别,从标题与内容父分类找到满足此分类的结果
    * 比较标题父分类与内容父分类，取两者之间最大概率那个且概率值大于0.8
  ii. 标题分类为unknow
    *
  iii.内容分类为unknow,即只有标题分类
    * furl判断只能用标题,采用标题分类结果,且父类概率大于0.5
    * furl能判断出父类别,且与标题父分类相同，直接采用
    * furl不能判断父类别,且标题父分类结果大于0.8

  2. 数据绝对不可用
  i. 去掉指定的无用url的数据
  ii. 去掉电商url数据
  iii. 去掉标题为"百度一下"
  iv. 去掉内容中以"ERROR"开头的文章
  v. 对小说内容的去除，furl = "https://m.baidu.com" ftitle = "第xx章"
   */
  def filterArticle(statDate: String) = {
    val sparkEnv = new SparkEnv("unionAllCat").sparkEnv
    val sql = s"select * from ${UrlConfig.unionDataTable} where stat_date=${statDate}"
    val sqlData = sparkEnv.sql(sql).drop("stat_date")
    val pat = new Regex("第[ 0-9]*章.*")
    //val st:StructType = sqlData.schema
    val data = sqlData.rdd.filter(r=>{
      //去掉不可用数据
      try{
        val furl = r.getAs[String]("furl")
        val ftitle = r.getAs[String]("ftitle").trim
        val fcontent = r.getAs[String]("fcontent").trim
        (UrlConfig.useless_url_arr ++ UrlConfig.ec_url_arr).filter(url=>{
          furl.indexOf(url) != -1
        }).length == 0 && {
          ftitle ==null || ftitle.indexOf("百度一下") == -1 ||
          ftitle.indexOf("热门城市") == -1 || ftitle.indexOf("关于安居客") == -1 ||
          ftitle.indexOf("赞(0) 评论") == -1 || pat.findFirstIn(ftitle).isEmpty
        } && {
          fcontent == null || !fcontent.startsWith("ERROR")
        }
      }catch{
        case _:Exception => true
      }
    }).map(r=>{
      val fid = r.getAs[Long]("fid")
      val ftitle = r.getAs[String]("ftitle")
      val src_type = r.getAs[String]("src_type")
      val furl = r.getAs[String]("furl")
      val title_pcat = r.getAs[String]("title_pcat")
      val title_pprob = r.getAs[Double]("title_pprob")
      val title_ccat = r.getAs[String]("title_ccat")
      val title_cprob = r.getAs[Double]("title_cprob")
      val content_pcat = r.getAs[String]("content_pcat")
      val content_pprob = r.getAs[Double]("content_pprob")
      val content_ccat = r.getAs[String]("content_ccat")
      val content_cprob = r.getAs[Double]("content_cprob")

      val special_label_url:Array[String] = UrlConfig.special_label_url_map.keySet.filter(r=>{
        furl.indexOf(r) != -1
      }).toArray

      val special_label = if(special_label_url.size == 1){
        UrlConfig.special_label_url_map(special_label_url(0))
      } else{
        ""
      }

      val is_only_use_title = UrlConfig.only_use_title_url_arr.filter(r=>{
        furl.indexOf(r) != -1
      }).length > 0


      val cat = if(title_pcat != "unknow" && content_pcat != "unknow"){
        if(title_ccat == content_ccat){
          title_ccat
        }else if(special_label != ""){
          if(special_label == title_pcat)
            if(title_ccat != null) title_ccat else special_label
          else if(special_label == content_pcat)
            if(content_ccat != null) content_ccat else special_label
          else
            special_label
        }else if(is_only_use_title && title_pprob > 0.5){
          title_ccat
        }else {
          if(title_pprob >= content_pprob && title_pprob > 0.8){
            if(title_ccat != null) title_ccat else title_pcat
          }else if( title_pprob < content_pprob && content_pprob > 0.8){
            if(content_ccat !=null) content_ccat else content_pcat
          }else{
            "unknow"
          }
        }
      }else if(title_pcat != "unknow"){
        if(special_label != "") {
          if (special_label == title_pcat)
            if(title_ccat != null) title_ccat else title_pcat
          else
            special_label
        }else if(is_only_use_title && title_pprob > 0.5){
          if(title_ccat != null) title_ccat else title_pcat
        }else {
          if(title_pprob > 0.8)
            if(title_ccat != null) title_ccat else title_pcat
          else
            "unknow"
        }
      }else if(content_pcat != "unknow"){
        if(special_label != "") {
          if (special_label == content_pcat)
            if(content_ccat != null) content_ccat else content_pcat
          else
            s"${special_label}_unknow"
        }else{
          if(content_pprob > 0.8) {
              if(content_ccat != null) content_ccat else content_pcat
            }
          else
            "unknow"
        }
      }else{
        "unknow"
      }
      (fid,ftitle,cat,src_type,
        furl,title_pcat,title_pprob,content_pcat,content_pprob)
    }).filter(_._3 !="unknow")

    val cols = Array("algo_ver","fid", "ftitle", "cat", "src_type",
      "furl","title_pcat","title_pprob","content_pcat","content_pprob")
    val colsType = Array("string","long", "string", "string", "string",
      "string","string","double","string","double")
    val st = DXPUtils.createSchema(cols.zip(colsType))
    val df: DataFrame = sparkEnv.createDataFrame(data.map(r => {
      Row("algo_basic",r._1, r._2, r._3, r._4,
        r._5,r._6,r._7,r._8,r._9)
    }), st).repartition(200)

    DXPUtils.saveDataFrame(df, etlTable, statDate, sparkEnv)
  }
}

