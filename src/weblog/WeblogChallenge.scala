/**
 * =========================================
 * Weblog 
 * =========================================
 * 
 * spark-shell --master local[4] --driver-memory 2g
 *
 * spark-submit --master local[4] target/Weblog-0.1.0-jar-with-dependencies.jar 3 14.141.255.74
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.HashMap
import java.util.Date
import scala.util.matching.Regex
import scala.collection.JavaConverters._
import java.util.Calendar

import org.apache.log4j.LogManager
import org.apache.log4j.Level

object Weblog extends java.io.Serializable {
  
  /**
   * The options (in order) are:
   *   - operation: [0|1|2|3|4|5|6|7|8]
   *   	   0: generate url / user profile data; 
   *       1: generate transition data; 
   *       2: generate transition data and simulate arbitrary user navigation
   *       3: simulate navigation for a IP; 
   *       4: generate time series data for server load
   *       5: generate server load data for regression
   *       6: predict load for next minute
   *       7: predict avg. session time for a IP by regression
   *       8: predict avg. unique URL visits for a IP by regression
   *   - IP: client IP
   */
  def main(args: Array[String]): Unit = {
    
    val log = LogManager.getRootLogger()
    log.setLevel(Level.ERROR)
    
    val conf = new SparkConf().setAppName("Weblog")
    val sc = new SparkContext(conf)

    val weblog = new Weblog()
    
    val debug_mode = false
    
    var func: Int = -1
    var ip: String = ""
    if (args.length > 0) func = args(0).toInt
    if (args.length > 1) ip = args(1)
    
    println("function: " + func + "; ip: " + ip)
    if (!(func >= 0 && func <= 8)) {
      println("Invalid operation " + func + ". Select from [ 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8]")
      return
    }
    
    if ((func == 3 || func == 7 || func == 8) && ip.length == 0) {
      println("Need IP for operation " + func)
      return
    }
    
    // load the log file and parse out individual fields

    var data_file = ""
    if (debug_mode) {
      // for test, just load the small batch
      data_file = "data/split/weblogaa"
    } else {
      data_file = "data/2015_07_22_mktplace_shop_web_log_sample2.log"
    }
    
    // file declarations...
    val outbase = "output"
    // All user session profile data
    val urls_file = outbase + "/urls.txt"
    val user_dur_file = outbase + "/user_duration.txt"
    val session_dur_file = outbase + "/session_dur.txt"
    val session_unique_url_counts_file = outbase + "/session_unique_url_counts.txt"
    val session_url_count_file = outbase + "/session_url_count.txt"
    
    // Transition probabilities file for Markov model
    val transitions_file = outbase + "/transitions.txt"
    
    // Server load prediction related files.
    val ts_file = outbase + "/per_sec_counts.txt"
    val features_file = outbase + "/server_load_features.txt"
    val lr_model_file = outbase + "/server_load_model"

    var rawData = sc.textFile(data_file)
    if (func == 3) {
      // if the operation is for a specific user/ip, it would be *much* efficient
      // to filter the raw data at the earliest.
      rawData = rawData.map(line => (line, line.split(" ")(2))).filter(line => line._2.startsWith(ip)).map(x => x._1)
      if (rawData.count == 0) {
        println("IP: " + ip + " not found...")
        return
      }
    }
    
    // a filter to remove some ill-formed requests
    val reqs = rawData.map(line => weblog.parse_request(line)).filter(req => !req.client_ip.startsWith("\""))

    val user_reqs = reqs.groupBy(req => req.client_ip)

    val user_profiles = weblog.get_user_profiles(user_reqs)

    val unique_urls = weblog.get_unique_urls(user_profiles)

    val output_url_info = (func == 0)
    if (output_url_info) {
      // output CSV: url_id,url,count_visited,avg_time_from_prev_url_in_session
      weblog.write_seq_to_file(
        unique_urls.map(url => url._2.id + "," + url._2.url + "," + url._2.count + "," + url._2.time_to_reach).collect,
        urls_file)
    }

    val output_profile_data = (func == 0)
    if (output_profile_data) {
      // output CSV: ip,number_of_sessions,avg_session_duration
      weblog.write_seq_to_file(
        user_profiles.sortBy(x => (-1.0) * x.avg_session_length).
          map(prof => prof.ip + "," + prof.max_sessions + "," + prof.avg_session_length).collect(),
        user_dur_file)

      // output CSV: ip,session_id,total_navigations,total_time
      weblog.write_seq_to_file(
        user_profiles.map(up => {
          up.session_dur.map(sd => (up.ip, sd._1, sd._3, sd._2))
        }).flatMap(x => x).sortBy(x => (-1) * x._4).map(sd => sd._1 + "," + sd._2 + "," + sd._3 + "," + sd._4).collect(),
        session_dur_file)

      // output CSV: ip,session_id,unique_url_count
      weblog.write_seq_to_file(
        user_profiles.map(up =>
          up.session_unique_url_counts.map(uu => up.ip + "," + uu._1 + "," + uu._2)).reduce((a, b) => a ++ b).sortBy(x => x),
        session_unique_url_counts_file)

      // output CSV: ip,session_id,num_url_visits,url
      weblog.write_seq_to_file(
        user_profiles.map(up =>
          up.session_url_count.map(uu => up.ip + "," + uu._1._1 + "," + uu._2 + "," + uu._1._2)).reduce((a, b) => a ++ b).sortBy(x => x),
        session_url_count_file)
    }

    val output_transitions = (func == 1)
    if (output_transitions) {
      val (id2url, url2id) = weblog.build_url_lookups(unique_urls.collect())
      val normalized_trans_counts = weblog.generate_transition_data(user_profiles, id2url, url2id)
      weblog.write_seq_to_file(
        normalized_trans_counts.collect().map(x => x._1 + "," + x._2 + "," + x._3 + "," + x._4),
        transitions_file)
    }

    if (func == 2) {
      val (id2url, url2id) = weblog.build_url_lookups(unique_urls.collect())
      weblog.simuate_markov(user_profiles, id2url, url2id)
    }

    if (func == 3) {
      
      // Note: below filtering is actually redundant now since 
      // we are filtering the rawData much earlier.
      val user_profile_ip = user_profiles.filter(up => up.ip == ip)
      
      val unique_urls_ip = weblog.get_unique_urls(user_profile_ip)
      unique_urls_ip.foreach(println)
      
      val (id2url_ip, url2id_ip) = weblog.build_url_lookups(unique_urls_ip.collect())
      
      val (tm, l, u) = weblog.simuate_markov(user_profile_ip, id2url_ip, url2id_ip)
      
      println("Avg Time: " + tm + ", avg nav length: " + l + ", avg unique url: " + u)
    }

    if (func == 4) {
      val ts = weblog.generate_ts_data(rawData)
      val dt_format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm")
      val ots = ts.toSeq.map(x => {
        val dt = dt_format.parse(x._1)
        val cal = java.util.Calendar.getInstance()
        cal.setTime(dt)
        (x._1, cal.get(java.util.Calendar.HOUR_OF_DAY)*60 + cal.get(java.util.Calendar.MINUTE), x._2)
      }).sortBy(_._1)
      weblog.write_seq_to_file(
        ots.map(tm => tm._1 + "," + tm._2 + "," + tm._3),
        ts_file)
    }
    
    if (func == 5) {
      val tmpreqs = reqs  //.filter(req => req.client_ip == ip)
      val instances = weblog.generate_load_regression_data(tmpreqs)
      weblog.write_seq_to_file(
        instances.map(inst => inst.label + "," + inst.features.toArray.mkString(",")),
        features_file)
      
      val training = sc.parallelize(instances.toSeq).cache()
      val algo = new org.apache.spark.mllib.regression.LinearRegressionWithSGD()
      val model = algo run training
      
      if (model != null) model.save(sc, lr_model_file)
      // Now, the above model might be used to predict server load...
      // Since we do not have enough data, we will stop here.
      // Ideally, we would like to have a separate test set and train set,
      // but without much data, it is not possible.
    }
    
    if (func == 6) {
      
      // just a test time that works on the provided log file.
      val in_time_str = "2015-07-22 09:03"
      
      // predict load for next minute: first get current time till minutes
      // uncomment the below in live environment
      //val in_time_str = (new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm")).format(new java.util.Date())
      
      val n = 5 // look five minutes before this period
      val pred_load = weblog.get_local_load_prediction(rawData, n, in_time_str)
      
      println("Predicted load at " + in_time_str + " is " + pred_load + " requests per sec.")
      
    }
    
    if (func == 7) {
      val avg_session_dur = weblog.predict_time_url_ip(user_profiles, ip, true)
      println("Predicted avg session duration for ip " + ip + " is " + avg_session_dur)
    }
    
    if (func == 8) {
      val avg_session_url = weblog.predict_time_url_ip(user_profiles, ip, false)
      println("Predicted avg unique urls for ip " + ip + " is " + avg_session_url)
    }
    
    sc.stop()
    
  }
}

class Weblog() extends java.io.Serializable {

    import java.io._
    import util.Random
    import org.apache.commons.math3.random.MersenneTwister
    import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution
    import org.apache.spark.mllib.linalg._
    import org.apache.spark.mllib.regression._
    
    import org.apache.spark.mllib.regression.LinearRegressionModel
    import org.apache.spark.mllib.regression.LinearRegressionWithSGD
    
    import org.apache.spark.mllib.tree.RandomForest
    import org.apache.spark.mllib.tree.model.RandomForestModel
    import org.apache.spark.mllib.util.MLUtils

    def reduceByKey[K, V](x: Seq[(K, V)], func: (V,V) => V): Seq[(K, V)] = {
        val map = collection.mutable.Map[K, V]()
        x.foreach { e =>
            val k = e._1
            val v = e._2
            map.get(k) match {
                case Some(pv) => {
                    map(k) = func(pv, v)
                }
                case None => {
                    map(k) = v
                }
            }
        }
        map.toSeq
    }
    
    def write_seq_to_file(data: Traversable[String], path: String) = {
        val writer = new PrintWriter(new File(path))
        data.foreach(x => writer.println(x))
        writer.close()
    }
    
    def parse_date(str_date: String): java.util.Date = {
        // we should really try to get the time zone as well, 
        // but Amazon's timezone format is a bit confusing...
        // NOTE: Input date should be of format:
        //   yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ
        // We will ignore the last three digits
        val D_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        val d_format = new java.text.SimpleDateFormat(D_FORMAT)
        
        //d_format.parse(str_date.substring(0, str_date.length-4))
        d_format.parse(str_date.substring(0, 23))
    }
    
    def maxDate(d1: java.util.Date, d2: java.util.Date): java.util.Date = {
        var mxd = d1
        if (d2.after(d1)) {
            mxd = d2
        }
        mxd
    }
    
    def minDate(d1: java.util.Date, d2: java.util.Date): java.util.Date = {
        var mnd = d1
        if (d2.before(d1)) {
            mnd = d2
        }
        mnd
    }
    
    def getDateDiff(start: java.util.Date, end: java.util.Date) : Long = {
        val diff_mil = end.getTime - start.getTime
        diff_mil / 1000
    }
    
    case class Request(
        timestamp: java.util.Date, 
        client_ip: String, 
        request: String, 
        url: String,
        time_to_reach: Long,
        session_id: Int)
    
    /**
     * Remove the redundant stuff from URLs...
     */
    def parse_url(req: String): String = {
        val PATTERN = """\"(GET|POST|HEAD|DELETE|PUT) https?://((www|shop|m)\.)?paytm.com:(80|443)([^?]*).*\"""".r
        var url = PATTERN.replaceAllIn(req, "$5")
        if (url.endsWith(" HTTP/1.1") || url.endsWith(" HTTP/1.0")) {
            url = url.substring(0, url.length-9)
        }
        url
    }
    
    /**
     * Urls contain lots of stuff which are irrelevant for determining
     * high-level user activities. We are taking a few liberties here
     * in sanitizing the requests so as to: 
     * 	1. reduce the number of unique urls to a manageable size 
     *         (important for the Markov navigation model) and, 
     *  2. get to a proper level of abstraction of site navigation.
     */
    def cardinal_url(url: String): String = {
        //val PAT1 = """/(products?)/[0-9]+/""".r
        //val PAT2 = """/product/[0-9]+/""".r
        val PAT3 = """/(orderdetail|summary|myorders|product|products|recharges)/[0-9]+""".r
        val PAT4 = """/shop/p/[^/]+""".r
        val PAT5 = """(/shop/(g|h)/)([^/]+)/.*""".r
        val PAT6 = """wp-content/(uploads|plugins|themes)/.*""".r
        val PAT7 = """wp-includes/.*""".r
        val PAT8 = """//?bus-tickets/.*""".r
        val PAT9 = """^/scripts/.*""".r
        
        var l = PAT3.replaceFirstIn(url, "/$1/*")
        l = PAT4.replaceFirstIn(l, "/shop/p/*")
        l = PAT5.replaceFirstIn(l, "$1$3")
        l = PAT6.replaceFirstIn(l, "wp-content/$1/*")
        l = PAT7.replaceFirstIn(l, "wp-includes/*")
        l = PAT8.replaceFirstIn(l, "/bus-tickets/*")
        l = PAT9.replaceFirstIn(l, "/scripts/*")
        l
    }
    
    def parse_request(line: String): Request = {
        val PATTERN = """ (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)""".r
        
        val l = PATTERN.split(line)
        
        val req_time = parse_date(l(0))
        
        val cip:Array[String] = l(2).split(':')
        val bip:Array[String] = l(3).split(':')
        val s_url = parse_url(l(l.length-4))
        Request(req_time, cip(0), s_url, cardinal_url(s_url), 0, 0)
    }
    
    case class UserProfile (
        ip: String,
        max_sessions: Int,
        reqs: Seq[Request],
        session_span: Seq[(Int, (java.util.Date, java.util.Date, Int))],
        session_dur: Seq[(Int, Long, Int)],
        session_url_count: Seq[((Int, String), Int)],
        session_unique_url_counts: Seq[(Int, Int)],
        avg_session_length: Double
    )
    
    /**
     * This is the most crucial method for user analysis. The first step
     * is to sessionalize the data. We group all records of a user, sort
     * them on time, and partition the records into session depending on
     * whether the time between two successive requests from a user (IP)
     * is greater than 30 mins.
     * 
     * Technical Note:
     * Scala has some repartition / sort within partition operations.
     * These features could help speed up the below sessionalization code.
     */
    def get_user_profiles(user_reqs: org.apache.spark.rdd.RDD[(String, Iterable[Request])]): 
                org.apache.spark.rdd.RDD[UserProfile] = {
        
        val max_session_gap = 30*60  // gap in seconds
        
        val user_profiles = user_reqs.map(user => {
            var rs: Seq[Request] = Seq[Request]()
            var prev_time = (new java.text.SimpleDateFormat("yyyy-MM-dd")).parse("1900-01-01")
            val ip = user._1
            val ureqs = user._2
            val oreqs = ureqs.toSeq.sortWith((x, y) => x.timestamp.before(y.timestamp))
            var sess_id = 0
            var started_session = false
            var session_start_time = prev_time
            for (req <- oreqs) {
                val dd = getDateDiff(prev_time, req.timestamp)
                if (dd > max_session_gap) {
                    sess_id = sess_id + 1
                    started_session = true
                    session_start_time = req.timestamp
                } else {
                    started_session = false
                }
                val time_to_reach: Long = if (started_session) 0L else dd
                rs = rs :+ req.copy(time_to_reach=time_to_reach, session_id=sess_id)
                prev_time = req.timestamp
            }
            val max_sessions = sess_id
            
            // get the min and max times of each user session
            val session_start_end = reduceByKey(
                rs.map(req => (req.session_id, (req.timestamp, req.timestamp, 1))), 
                (a: (java.util.Date, java.util.Date, Int), 
                 b: (java.util.Date, java.util.Date, Int)) => 
                        (minDate(a._1, b._1), maxDate(a._2, b._2), a._3 + b._3))
                        
            // get the session durations and sort by session time in descending order.
            val session_dur = session_start_end.map(
                sess => (sess._1, getDateDiff(sess._2._1, sess._2._2), sess._2._3)
            ).toSeq.sortBy((-1)*_._2)
            
            // Get the average session duration for a user.
            // This info could be moved out of UserProfile if
            // not required very frequently.
            val total_user_dur = session_dur.reduce(
                    (a, b) => (0, a._2 + b._2, a._3 + b._3) // put dummy 0 as session id
                )
            val avg_session_length = total_user_dur._2*1.0 / session_dur.length
            
            // Now get count of each unique url in a session
            // We do not really need to store this info un UserProfile
            // structure since it is NOT a frequently requested info.
            // Some data structure refactoring should remove this out.
            // The saving grace is that the Strings URLs are most likely
            // going to be shared (by reference) and hence not take too 
            // much memory.
            val session_url_count = reduceByKey(
                rs.map(req => ((req.session_id, req.url), 1)),
                (a:Int, b:Int) => a + b).sortBy(_._1)
            
            // Now get count of unique urls per session.
            // Similar to session_url_count, session_unique_url_counts
            // should also be moved out of UserProfile.
            val session_unique_url_counts = reduceByKey(
                session_url_count.map(urls => (urls._1._1, 1)),
                (a:Int, b:Int) => a + b).sortBy(_._1)
            
            UserProfile(ip, max_sessions, rs, session_start_end, 
                        session_dur, session_url_count, 
                        session_unique_url_counts, avg_session_length)
        })
        
        user_profiles
    
    }
    
    case class UrlInfo (id: Int, url: String, count: Int, time_to_reach: Double)
    
    def get_unique_urls(user_profiles: org.apache.spark.rdd.RDD[UserProfile]):
            org.apache.spark.rdd.RDD[(Int, UrlInfo)] = {
      
        val unique_urls = user_profiles.map(up => up.reqs).flatMap(x => x).
          map(req => (req.url, (req.time_to_reach, 1))).
          reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).
          sortBy(x=>(-1)*x._2._2).
          zipWithIndex.map(url => (url._2.toInt, UrlInfo(url._2.toInt, url._1._1, 
                                                         url._1._2._2, url._1._2._1*1.0/url._1._2._2)))
        unique_urls
    }
    
    def build_url_lookups(dat: Iterable[(Int, UrlInfo)]): (Map[Int, UrlInfo], Map[String, Int]) = {
        val id2url = collection.mutable.Map[Int, UrlInfo]()
        val url2id = collection.mutable.Map[String, Int]()
        dat.foreach(url => {
            id2url.put(url._1, url._2)
            url2id.put(url._2.url, url._1)
        })
        val term_url_id = -1 // id2url.keys.reduce(Math.max(_, _)) + 1
        id2url.put(term_url_id, UrlInfo(term_url_id, "", 0, 0))  // terminal state
        url2id.put("", term_url_id)  // terminal state
        (collection.immutable.Map[Int, UrlInfo](id2url.toList: _*),
         collection.immutable.Map[String, Int](url2id.toList: _*))
    }
    
    /**
     * To predict the next url for user, we consider a Markovian model.
     * 1. Requires computing transition probabilities from current url to next.
     * 2. The data might be for a specific user - in which case the
     * 	  user_profiles should be filtered on client IP.
     * 3. Once the probabilities have been computed, we simulate navigation
     *    and compute required statistics such as avg. session length,
     *    unique urls visited, etc.
     */
    
    /**
     * Computes the transition probabilities from historical data.
     * Assumes that the user_profiles have user requests ordered by timestamp.
     */
    def generate_transition_data(user_profiles: org.apache.spark.rdd.RDD[UserProfile], 
             id2url: Map[Int, UrlInfo], url2id: Map[String, Int]): org.apache.spark.rdd.RDD[(Int, Int, Double, Double)] = {
        
        val user_transitions = user_profiles.map(up => {
            var transitions: Seq[((Int, Int), (Int, Long))] = Seq[((Int, Int), (Int, Long))]()
            var s = 0
            var prev_urlid = -2 // a marker for start of navigation
            var prev_time = new java.util.Date()
            var started_session = false
            // Note: up.reqs are already sorted by timestamp.
            // Hence we can simply traverse in order.
            for (req <- up.reqs) {
                val dd = getDateDiff(prev_time, req.timestamp)
                if (req.session_id == s) {
                    started_session = false
                } else {
                    prev_urlid = -2
                    started_session = true
                }
                val time_to_reach: Long = if (started_session) 0L else dd
                val curr_urlid = url2id(req.url)
                transitions = transitions :+ ((prev_urlid, curr_urlid), (1, time_to_reach))
                prev_urlid = curr_urlid
                prev_time = req.timestamp
                s = req.session_id
            }
            transitions = transitions :+ ((prev_urlid, -1), (1, 0L))
            transitions
        }).flatMap(x=>x)
        
        val trans_count = user_transitions.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        
        val from_counts_tmp = collection.mutable.Map[Int, Int]()
        trans_count.map(x => (x._1._1, x._2._1)).reduceByKey(_+_).collect().foreach(x => from_counts_tmp.put(x._1, x._2))
        val from_counts = collection.immutable.Map[Int, Int](from_counts_tmp.toList: _*)
        
        val normalized_trans_counts = trans_count.
                map(x => (x._1._1, x._1._2, x._2._1*1.0/Math.max(1, from_counts(x._1._1)), x._2._2*1.0/x._2._1)).
                sortBy(_._1)
        
        normalized_trans_counts
    }
    
    /**
     * Data structure to store transition probabilities for simulating user navigation
     */
    case class TransitionInfo (
        id: Int, 
        to_ids: Array[Int], 
        to_probs: Array[Double], 
        durs: Array[Double], 
        multinomial: org.apache.commons.math3.distribution.EnumeratedIntegerDistribution)
    
    /**
     * Creates TransitionInfo objects from raw flat transition data.
     * The raw data is computed by generate_transition_data()
     */
    def build_transition_lookups(raw_trans: org.apache.spark.rdd.RDD[(Int, Int, Double, Double)]): Map[Int, TransitionInfo] = {
        val tmp = raw_trans.map(x => {
            (x._1.toInt, (x._1.toInt, x._2.toInt, x._3.toDouble, x._4.toDouble))
        }).groupByKey()
        val transinfo = tmp.map(tr => {
            var dests = Array[Int]()
            var destidxs = Array[Int]()
            var probs = Array[Double]()
            var durs = Array[Double]()
            var i = 0
            tr._2.foreach(x => {
                dests = dests :+ x._2
                destidxs = destidxs :+ i
                probs = probs :+ x._3
                durs = durs :+ x._4
                i += 1
            })
            val rand = new MersenneTwister(42+util.Random.nextInt)
            // the below creates structures which will be used for multinomial sampling
            // for generating next states from transition probabilities
            val multinomDist = new EnumeratedIntegerDistribution(rand, destidxs, probs)
            (tr._1, TransitionInfo(tr._1, dests, probs, durs, multinomDist))
        })
        val id2dest = collection.mutable.Map[Int, TransitionInfo]()
        transinfo.collect().foreach(x => id2dest.put(x._1, x._2))
        collection.immutable.Map[Int, TransitionInfo](id2dest.toList: _*)
    }
    
    /**
     * The actual work of simuation of user navigation.
     */
    def simulate_nav(start_id: Int, transinfo: Map[Int, TransitionInfo], 
                     id2url: Map[Int, UrlInfo]): (Double, Int, Int) = {
        var state = transinfo(start_id)
        var l = 0
        var tm = 0.0
        var ended = false
        val visited = collection.mutable.Map[Int, Int]()
        if (start_id >= 0) visited.put(start_id, start_id)
        while (l < 1000 && !ended) {
            val to_idx = state.multinomial.sample()
            val to_id = state.to_ids(to_idx)
            if (to_id >= 0) visited.put(to_id, to_id)
            val url = if (to_id < 0) "" else id2url(to_id).url
            //println("\t" + state.to_ids.mkString(" "))
            tm += state.durs(to_idx)
            if (to_id == -1 || !transinfo.exists(_._1 == to_id)) {
                //println("Session End...")
                ended = true
            } else if (l == 100) {
                //println("Very long session...")
                //ended = true
            } else {
                state = transinfo(to_id)
            }
            l += 1
        }
        (tm, l, visited.size)
    }

    def simulate_nav_multiple(start_id: Int, n: Int, transinfo: Map[Int, TransitionInfo],
        id2url: Map[Int, UrlInfo]): (Double, Double, Double) = {
        var tot_tm = 0.0
        var tot_len = 0
        var tot_unq = 0
        for (i <- 1 to n) {
          val (tm, l, u) = simulate_nav(start_id, transinfo, id2url)
          tot_tm += tm
          tot_len += l
          tot_unq += u
        }
        (tot_tm / n, tot_len * 1.0 / n, tot_unq * 1.0 / n)
    }
    
    /**
     * Here we simulate user navigation for a bunch of times and then
     * average statistics over these simulations.
     */
    def simuate_markov(user_profiles: org.apache.spark.rdd.RDD[UserProfile], 
             id2url: Map[Int, UrlInfo], url2id: Map[String, Int]): (Double, Double, Double) = {
        
        val n_sims = 200
        val start_url_id = -2  // -2 is default initial state
        
        val raw_trans = generate_transition_data(user_profiles, id2url, url2id)
    
        val transinfo = build_transition_lookups(raw_trans)
    
        val (tm, l, u) = simulate_nav_multiple(start_url_id, n_sims, transinfo, id2url)
        
        (tm, l, u)
        
    }
    
    /**
     * Here we predict the session time /  # unique urls by RandomForest regression.
     * I am skeptical about it's usefulness. But cursory exploration on the data revealed
     * there is some predictive power in this treatment.
     */
    def predict_time_url_ip(user_profiles: org.apache.spark.rdd.RDD[UserProfile], 
        ip: String, pred_sess_time: Boolean) : Double = {
        var features: org.apache.spark.rdd.RDD[org.apache.spark.mllib.regression.LabeledPoint] = null
      
        if (pred_sess_time) {
            // session durations
            features = user_profiles.map(up => {
                up.session_dur.map(sess => {
                    val ipp = up.ip.split("\\.")
                    org.apache.spark.mllib.regression.LabeledPoint(
                        sess._2,
                        org.apache.spark.mllib.linalg.Vectors.dense(
                            Array(ipp(0).toDouble,ipp(1).toDouble,ipp(2).toDouble,ipp(3).toDouble)
                        )
                    )
                })
            }).flatMap(x=>x)
        } else {
            // unique URLs
            features = user_profiles.map(up => {
                up.session_unique_url_counts.map(sess => {
                    val ipp = up.ip.split("\\.")
                    org.apache.spark.mllib.regression.LabeledPoint(
                        sess._2,
                        org.apache.spark.mllib.linalg.Vectors.dense(
                            Array(ipp(0).toDouble,ipp(1).toDouble,ipp(2).toDouble,ipp(3).toDouble)
                        )
                    )
                })
            }).flatMap(x=>x)
        }
        
        val categoricalFeaturesInfo = Map[Int, Int]()
        val numTrees = 20 // Use more in practice.
        val featureSubsetStrategy = "auto" // Let the algorithm choose.
        val impurity = "variance"
        val maxDepth = 4
        val maxBins = 32
        
        val splits = features.randomSplit(Array(0.7, 0.3))
        val (trainingData, testData) = (splits(0), splits(1))
        
        val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
          numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
        
        /*
        // Evaluate model on train instances and compute train error
        val labelsAndPredictions = trainingData.map { point =>
          val prediction = model.predict(point.features)
          (point.label, prediction)
        }
        val trainingMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
        println("Training Mean Squared Error = " + trainingMSE)
        */
        
        // Evaluate model on test instances and compute test error
        val labelsAndPredictions = testData.map { point =>
          val prediction = model.predict(point.features)
          (point.label, prediction)
        }
        val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
        
        println("Test Mean Squared Error = " + testMSE)
        
        val ipp = ip.split("\\.")
        val ip_features = org.apache.spark.mllib.linalg.Vectors.dense(
                        Array(ipp(0).toDouble,ipp(1).toDouble,ipp(2).toDouble,ipp(3).toDouble)
                    )
        val prediction = model.predict(ip_features)
        prediction
    }
    
    /**
     * Predict the expected server load (requests/sec) in the next minute.
     * 
     * This might be done in a number of ways of which three explored here are:
     * 1. A simplistic model that has features like (day-of-week, time slice of day (15 mins), 
     * 	  week day / not, month, week-of-month, etc.)
     * 2. A timeseries model where load at a particular time is an ARMA model.
     * 3. Report the average load from last n minutes. 'n' could be parameterized.
     */
    
    /**
     * Predict server load: Approach 1
     * ===============================
     * 
     * Note: This approach would not work for our case because we have only about 3 hrs of logs.
     */
    def get_simple_date_features(date: java.util.Date): (Int, Int, Int, Int) = {
        val cal = java.util.Calendar.getInstance()
        cal.setTime(date)
        //val fiver_hour =  (cal.get(java.util.Calendar.MINUTE) + 5) / 5
        (cal.get(java.util.Calendar.MONTH), 
            cal.get(java.util.Calendar.WEEK_OF_MONTH), 
            cal.get(java.util.Calendar.DAY_OF_WEEK),
            cal.get(java.util.Calendar.HOUR_OF_DAY))
    }
    
    def get_dummy_encoding[A](levels: Seq[A]): Map[A, Array[Double]] = {
        val encs = collection.mutable.Map[A, Array[Double]]()
        val n = levels.length
        var l = 0
        for (i <- levels) {
            val enc = Array.fill[Double](math.max(1, n-1))(0)
            if (n == 1) {
                enc(l) = 1
            } else if (l > 0) {
                enc(l - 1) = 1.0
            }
            encs.put(i, enc)
            l += 1
        }
        collection.immutable.Map(encs.toList: _*)
    }
    
    /**
     * We use DUMMY encoding instead of one-hot-encoding (OHE) because OHE
     * could make the design matrix singular in the presence of an intercept
     * term and therefore is unsuitable for linear models. (DUMMY represents
     * n levels of a categorical feature by a binary vector of length n-1
     * whereas OHE represents n levels by a binary vector of length n)
     * 
     * Note: The time-related features are being treated as categorical in this
     * regression approach.
     * 
     * We could consider broadcasting/caching an instance of the below class  
     * for efficiency, but this data structure is pretty small and hence we 
     * might not need to go to such lengths.
     */
    case class DummyEnc (
        month_enc: Map[Int, Array[Double]] = get_dummy_encoding(0 to 11),
        week_of_month_enc: Map[Int, Array[Double]] = get_dummy_encoding(1 to 5),
        day_of_week_enc: Map[Int, Array[Double]] = get_dummy_encoding(1 to 7),
        hour_of_day_enc: Map[Int, Array[Double]] = get_dummy_encoding(0 to 23),
        fiver_hour_enc: Map[Int, Array[Double]] = get_dummy_encoding(1 to 12))
    
    def encode_simple_date_features(s: (Int, Int, Int, Int), dummies: DummyEnc): Array[Double] = {
        dummies.month_enc(s._1) ++ dummies.week_of_month_enc(s._2) ++ 
            dummies.day_of_week_enc(s._3) ++ dummies.hour_of_day_enc(s._4) // ++ dummies.fiver_hour_enc(s._5)
    }
    
    def generate_load_regression_data(reqs: org.apache.spark.rdd.RDD[Request]): 
                                    Iterable[org.apache.spark.mllib.regression.LabeledPoint] = {
    
        val per_interval_counts = reqs.map(req => (get_simple_date_features(req.timestamp), 1)).
            countByKey()
        
        val dm = DummyEnc()
        val insts = per_interval_counts.map(x => org.apache.spark.mllib.regression.LabeledPoint(1.0*x._2, 
                          org.apache.spark.mllib.linalg.Vectors.dense(encode_simple_date_features(x._1, dm))))
        
        insts
    }
    
    /**
     * Predict server load: Approach 2
     * ===============================
     * 
     * Generate time series data for predicting server load for the next minute.
     * 
     * Note: A plot of per-minute server load shows that the load is 'spikey'.
     * This approach might not be suitable for the log file provided.
     */
    
    def parse_log_time_2_date(line: String): java.util.Date = {
      parse_date(line.substring(0, 27))
    }
    
    def parse_log_time_2_string(line: String): String = {
      line.substring(0, 16).replace("T", " ")
    }
    
    def generate_ts_data(rawData: org.apache.spark.rdd.RDD[String]): scala.collection.Map[String,Double] = {
        val rawDataFull = rawData
        
        val dt_format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm")
        //val per_sec_counts = rawDataFull.map(line => (dt_format.format(parse_log_time_2_date(line)), 1)).countByKey().
        //    map(x => (x._1, 1.0*x._2/60))
        val per_sec_counts = rawDataFull.map(line => (parse_log_time_2_string(line), 1)).countByKey().
            map(x => (x._1, 1.0*x._2/60))
        per_sec_counts
    }
    
    /**
     * Predict server load: Approach 3
     * ===============================
     * 
     * Filter requests from past n minutes till last one minute and compute 
     * average load within this interval.
     * 
     * Note: To predict next minute we are not using the current minute because
     * the current minute's data is not complete.
     */
    
    def get_local_load_prediction(rawData: org.apache.spark.rdd.RDD[String], n: Int, in_time_str: String): Double = {
      val dt_format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm")
      val in_time = dt_format.parse(in_time_str.substring(0, 16).replace("T", " "))
      val cal = java.util.Calendar.getInstance()
      cal.setTime(in_time)
      cal.add(java.util.Calendar.MINUTE, -n)
      val start_time = cal.getTime()
      cal.setTime(in_time)
      cal.add(java.util.Calendar.MINUTE, -1)
      val end_time = cal.getTime()
      
      val start_time_str = dt_format.format(start_time).replace(" ", "T")
      val end_time_str = dt_format.format(end_time).replace(" ", "T")
      
      val window_counts = rawData.map(line => (line.substring(0, 16), 1)).
        filter(rec => rec._1 >= start_time_str && rec._1 <= end_time_str).
          count()
          
      window_counts * 1.0 / (n * 60)
    }
    
}

