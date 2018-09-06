package com.meizu.algo.browser
import java.io.{InputStream, InputStreamReader}
import java.util
import java.io.BufferedReader

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
object UrlConfig {
  // for simhash
  var isDebug: Boolean = false
  val business ="url"
  val segTable = s"algo.dxp_${business}_simhash_seg"
  val hashTable = s"algo.dxp_${business}_simhash_hash"
  val sameTable = s"algo.dxp_${business}_simhash_same"
  val distanceTable = s"algo.dxp_${business}_simhash_distance"
  val dupTable = s"algo.dxp_${business}_simhash_dup_threshold_10"


  // for fasttext
  val dataTable = s"algo.dxp_${business}_fasttext_fid_title_content_day"

  val titleSegTable = s"algo.dxp_${business}_fasttext_title_seg_day"
  val contentSegTable = s"algo.dxp_${business}_fasttext_content_seg_day"

  val titlePcatTable = s"algo.dxp_${business}_fasttext_title_pcat_day"
  val contentPcatTable = s"algo.dxp_${business}_fasttext_content_pcat_day"
  val titleCcatTable = s"algo.dxp_${business}_fasttext_title_ccat_day"
  val contentCcatTable = s"algo.dxp_${business}_fasttext_content_ccat_day"
  val unionCattable = s"algo.dxp_${business}_union_cat_day"

  // merge all data and filter
  val unionDataTable = s"algo.dxp_${business}_union_data_day"
  val etlTable = s"algo.dxp_${business}_etl_day"

  val useless_url_file = "/browser/useless_url.txt"
  val special_label_url_file = "/browser/special_label_url.txt"
  val only_use_title_url_file = "/browser/only_use_title_url.txt"
  val ec_url_file = "/browser/ec_url.txt"

  val useless_url_arr:Array[String] =
    inputStreamToStringArray(useless_url_file).filter(_.trim.length > 0)
  val only_use_title_url_arr:Array[String] =
    inputStreamToStringArray(only_use_title_url_file).filter(_.trim.length > 0)
  val ec_url_arr:Array[String] =
    inputStreamToStringArray(ec_url_file).filter(_.trim.length > 0)
  val special_label_url_map:Map[String,String] =
    inputStreamToStringArray(special_label_url_file).filter(_.trim.length > 0).map(r=>{
      val arr = r.split("[ \t]+")
      if(arr.length == 2){
        (arr(0).trim, arr(1).trim)
      }else{
        ("","")
      }
    }).filter(r=>{
      r._1.length > 0 && r._2.length > 0
    }).toMap


  def inputStreamToStringArray(path:String): Array[String] = {
    val is: InputStream = this.getClass.getResourceAsStream(path)
    val br = new BufferedReader(new InputStreamReader(is))

    println("*"*20)
    println(s"file:${path}")
    val ab = new ArrayBuffer[String]();
    var line = br.readLine()
    while(line != null){
      if(line.trim != ""){
        ab.append(line)
        println(line)
      }
      line = br.readLine()
    }
    ab.toArray
  }

  def main(args: Array[String]): Unit = {
    val arr:Array[String] = inputStreamToStringArray("/browser/special_label_url.txt")
    for(i <- arr){
      println(i)
    }
  }
}
