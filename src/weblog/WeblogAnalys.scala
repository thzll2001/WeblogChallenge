package weblog

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.util.regex.Pattern
import java.io.PrintWriter
import java.io.File
import org.apache.spark.storage.StorageLevel
/**
     * 
     * The code is for the Task Q1-Q7
     * Developing environment is windows, spark,scala,jdk1.8,python
     * Q1-Q4 is read the data and  data Pre-Procession in Spark,transform the data to easy understand format 
     * the session is generate by the algorithm of "CART"(15 Minute),if we want to get higer accuracy we can use (1 minute)
     * the Q5 is output data to disk ,the prediction is implemented by python , this is a time series problem ,so we 
     * use python Deep learning technique to predict the next second session,the LSTM is very well tools in prediction for time
     * series data.
     * 
     *  
     */
object WeblogChangeAnalytical {

  /* parase_url
   * Filt the input url ,output the right http or https url
   * */
  def parase_url(url: String): String = {
    val httpreg = "(http|https)(.*\\s)"
    val p = Pattern.compile(httpreg)
    val matcher = p.matcher(url)
    var httpurl = ""
    if (matcher.find()) {
      httpurl = matcher.group()
    }
    (httpurl)
  }
  def write_seq_to_file(data: scala.collection.Map[String, Long], path: String) = {
    val writer = new PrintWriter(new File(path))
    data.foreach(x => writer.println(x))
    writer.close()
  }
  def write_seq_to_file2(data: Map[Int, String], path: String) = {
    val writer = new PrintWriter(new File(path))
    data.foreach(x => writer.println(x))
    writer.close()
  }
  val time_format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSS")

  /*Data pre-process ,we can us "map" to get the new format data ,it's easy to use. 
   * 1.To splite the record to different fields
   * 2. splite the time to segament and generate the sessionid(15m as a session)
   * 3. The algorithm is to group the time to segments
   * */

  def parse(row: String) = {
    val values = row.split(" ")
    //    val time = time_format.parse(values(0)).getTime()
    val ip = values(2).split(":")(0)
    val rescode = values(7)
    val reqcode = values(8)
    //    val url_raw = parase_url(
    val url_raw = values(12)

    val mmtime = values(0)
    //      new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm").format(time)
    var time_mm = 0
    var sstime = ""

    if (mmtime.length() > 2) {
      time_mm = mmtime.substring(14, 16).toInt
    } else {
      println("Time format is not right")
    }

    sstime = mmtime.substring(0, 19)

    val ymdh = mmtime.substring(0, 13)
    var newymdh = ""
    if (time_mm >= 0 && time_mm < 16) {
      newymdh = ymdh + ":01"
    } else if (time_mm > 15 && time_mm < 31) {
      newymdh = ymdh + ":15"
    } else if (time_mm > 30 && time_mm < 46) {
      newymdh = ymdh + ":35"
    } else if (time_mm > 31 && time_mm < 61) {
      newymdh = ymdh + ":45"
    }

    var host = ""
    val p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+")
    val newurl = url_raw

    val matcher = p.matcher(url_raw)

    if (matcher.find()) {
      host = matcher.group()
    }
    val pre_fix = host.substring(0, 3)
    if (pre_fix != "www") {
      host = "www." + host
    }
    sstime = sstime.replace(":", "")
    val sessionid = host + "#" + ip + "#" + newymdh
    (ip, mmtime, rescode, reqcode, url_raw, sessionid, newymdh, sstime, host)
  }

//  val session_seqtime = 15 * 60

  /**
     * 
     * All task from Q1 to Q7 
     * spark is running in local mode,Q*_out is set for the output path ,the results will output to it.
     * 
     */

  def LogAnalys(): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("task1").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel("WARN")
    val spark = SparkSession
      .builder()
      .appName("weblog")
      .enableHiveSupport()
      .getOrCreate()

    println("=========================Start==================================")
    //read the file
    val logpath = "F:/geekspeakwork/code/wordmtrix/testmatrix/rl/challenge/WeblogChallenge/data/log/2015_07_22_mktplace_shop_web_log_sample2.log"
    val logrdd = sc.textFile(logpath, 1)

    logrdd.persist(StorageLevel.DISK_ONLY_2)

    /*parse the columns of log */
    val session_rdd = logrdd.map(r => parse(r))

    session_rdd.persist(StorageLevel.MEMORY_AND_DISK)

    /*Q1 Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session. https://en.wikipedia.org/wiki/Session_(web_analytics) */

    val sortrdd_session_ip = session_rdd.map(f => (f._6, 1)).sortByKey()
    val sessionrdd_hits_each = sortrdd_session_ip.countByKey()

    
    val Q1_out = "f:/Q1.txt"
    write_seq_to_file(sessionrdd_hits_each,Q1_out)
    //    session_rdd.foreach(x=>println(x._5))
    println("please check output file:" + Q1_out)

    /* Q2 Determine the average session time */

    val Q2_out = "f:/Q2.txt"
    val session_total_count = sortrdd_session_ip.count() * 15 * 60
    val average_session_time = session_total_count / sessionrdd_hits_each.size
    //val average_session_time=sortrdd_session_ip.map(a => (a._1, (a._2, 1))).reduceByKey((a,b) => (a._1+b._1,a._2+b._2)).map(t => (t._1,t._2._1/t._2._2))
   
    println("average_session_time:" + average_session_time + "seconds")

    /* Q3 Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.q*/
    val url_session_ip = session_rdd.map(f => (f._5 + "#" + f._6, 1)).sortByKey()

    val url_hits_each = url_session_ip.countByKey()
    val Q3_out = "f:/Q3"

    write_seq_to_file(url_hits_each, Q3_out + ".txt")
    println("please check output file:" + Q3_out)

    /*Q4 Find the most engaged users, ie the IPs with the longest session times sessionid+ip */

    val ip_session = session_rdd.map(f => (f._5 + "#" + f._1, 1)).sortByKey()
    val ip_session_count = ip_session.reduceByKey(_ + _)
    val ip_session_value = ip_session_count.map(f => (f._2, f._1)).sortByKey(false)
    val Q4_out = "f:/Q4"
    val max_v = ip_session_value.take(1)
    ip_session_value.saveAsTextFile(Q4_out)

    println("the most engaged users ", max_v)
    println("please check output directory :" + Q4_out)

    println("=========================End Analysis Part1==================================")

    println("=========================Start Prediction Part2==================================")

    /*
    part2 :Q5 .Predict the expected load (requests/second) in the next minute
    caculate the request /second of website
    f._8 is the second time segaments, transfer it to be time series data we can sort by time
    and output it to file ,we can use python to read this and use RNN to prediction the request/second.
    *
    */
    val request_second = session_rdd.map(f => (f._8, 1)).sortByKey()
    println("sort_____________________")
    val request_second_count = request_second.reduceByKey(_ + _).sortByKey()

    val size = request_second_count.count()
    //out put it for python prediction
    val req_sec_out = "f:/req_sec"
    request_second_count.saveAsTextFile(req_sec_out)
    println("The prediction is implement by python in LSTM ,output the file for prediction:" + req_sec_out)

    
    //Q6. Predict the session length for a given IP
    //if give a ip, it's a search problem, filt the data by IP, we will get a small dataset then sum of the length of session
    
    println("please input the ip:")
    val ip = readLine("Please input  ip fro seeion length:")
    //filt the data by Ip
    val ipsession = session_rdd.filter(x => x._1 == ip)

    // host +15 minutes time segement+ ip as key
    val oneip_session = ipsession.map(f => (f._9 + f._2 + f._1, 1))
    val total_sessionip = oneip_session.count()
    val oneip_session_count = oneip_session.reduceByKey(_ + _)
    val session_len = oneip_session_count.count()
    println("total session length is:", session_len)

    //Q7. Predict the number of unique URL visits by a given IP

    val ip2 = readLine("Please input  ip for url:")
    //filt the data by url
    val ipsession2 = session_rdd.filter(x => x._1 == ip2)

    // url+ ip as key

    val oneip_session_url = ipsession2.map(f => (f._4 + f._1, 1))
    val total_number_oneip = oneip_session_url.count()
    println("number of url is:", total_number_oneip)

  }



  def main(args: Array[String]): Unit = {
    LogAnalys()


  }

}
