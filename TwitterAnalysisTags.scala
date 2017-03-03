package com.twitter.analysis

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
/**
  * Created by Bala.
  */
object TwitterAnalysisTags {

  def main(args: Array[String]) {
    /*if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }*/

    // Set logging level if log4j not configured (override by adding log4j.properties to classpath)
    /*if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }*/

    /*val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)*/
    //val filters = args.takeRight(args.length - 4)
    val filters = Seq("london")
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", "Zog4jvIBhKFwm9yNNbwBLdOme")
    System.setProperty("twitter4j.oauth.consumerSecret", "4hOSixh9zc2IT99sYJebyW62JykM1F5RRc1yBiaUi4jw8NaGmr")
    System.setProperty("twitter4j.oauth.accessToken", "74962611-clj1DVjIy7lX1qRNVCcX5KzNZCDbRDB3k5r9fpDd8")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "fDuBa5X0AHUuIv60dEupNHyaXfvhMoQSEmAaI9diuus2M")

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val hashTags = stream.flatMap(status => status.getText.split("\r\n"))

    hashTags.print()
    println("-----")


    /*val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))


    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })
*/
    ssc.start()
    ssc.awaitTermination()
  }

}
