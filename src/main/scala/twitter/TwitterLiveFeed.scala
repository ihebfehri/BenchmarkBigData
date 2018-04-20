package twitter

//
//import org.apache.spark.internal.config
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.streaming.twitter.TwitterUtils
//import twitter4j.Status
//import twitter4j.auth.OAuthAuthorization
//import twitter4j.conf.ConfigurationBuilder
//
//object TwitterSentimentScore extends App {
//
//  // You can find all functions used to process the stream in the
//  // Utils.scala source file, whose contents we import here
//  import Utils._
//
//
//  // twitter4j oauth
//  val consumerKey = "pcB2ux0IGNRPnENQScJeWCjRK"
//  val consumerSecret = "GB8XovWvWv3hj6SzhlJd1oyPU30j1F0kz7WlMnm1FqSxOqGA04"
//  val accessToken = "268885311-1AieR67Ze3QpqmKfmrwY9tQqDSyDv8wLIquvj0oy"
//  val accessTokenSecret = "wfyzOHztJ2TYOhQ9XtDyibCQaPNL5iMAxjPha5vE4BCjo"
//
//  var a = new Array [String](4)
//  a(0) = consumerKey
//  a(1) = consumerSecret
//  a(2) = accessToken
//  a(3) = accessTokenSecret
//
//  // Tried using twitter4j.properties but spark did not recognise it when i converted to jar.
//  // I manually added it in the code so it would work in.
//  a = args.take(4)
//  val filters = args.takeRight(args.length - 4)
//  val cb = new ConfigurationBuilder
//  cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
//    .setOAuthConsumerSecret(consumerSecret)
//    .setOAuthAccessToken(accessToken)
//    .setOAuthAccessTokenSecret(accessTokenSecret)
//  val auth = new OAuthAuthorization(cb.build)
//
//
//  // set an application name and master
//  // If no master is given as part of the configuration we
//  // will set it to be a local deployment running an executor per thread
//  val sparkConfiguration = new SparkConf().
//    setAppName("spark-twitter-stream-example").
//    setMaster(sys.env.get("spark.master").getOrElse("local[*]"))
//
//  // Let's create the Spark Context using the configuration we just created
//  val sparkContext = new SparkContext(sparkConfiguration)
//
//  // Now let's wrap the context in a streaming one, passing along the window size
//  val streamingContext = new StreamingContext(sparkContext, Seconds(5))
//
//  // Creating a stream from Twitter (see the README to learn how to
//  // provide a configuration to make this work - you'll basically
//  // need a set of Twitter API keys)
//  val tweets: DStream[Status] = TwitterUtils.createStream(streamingContext, Some(auth) )
//
//
//
//  // To compute the sentiment of a tweet we'll use different set of words used to
//  // filter and score each word of a sentence. Since these lists are pretty small
//  // it can be worthwhile to broadcast those across the cluster so that every
//  // executor can access them locally
//  val uselessWords = sparkContext.broadcast(load("/stop-words.dat"))
//  val positiveWords = sparkContext.broadcast(load("/pos-words.dat"))
//  val negativeWords = sparkContext.broadcast(load("/neg-words.dat"))
//
//  // Let's extract the words of each tweet
//  // We'll carry the tweet along in order to print it in the end
//  val textAndSentences: DStream[(TweetText, Sentence)] =
//    tweets.
//      map(_.getText).
//      map(tweetText => (tweetText, wordsOf(tweetText)))
//
//  // Apply several transformations that allow us to keep just meaningful sentences
//  val textAndMeaningfulSentences: DStream[(TweetText, Sentence)] =
//    textAndSentences.
//      mapValues(toLowercase).
//      mapValues(keepActualWords).
//      mapValues(words => keepMeaningfulWords(words, uselessWords.value)).
//      filter { case (_, sentence) => sentence.length > 0 }
//
//  // Compute the score of each sentence and keep only the non-neutral ones
//  val textAndNonNeutralScore: DStream[(TweetText, Int)] =
//    textAndMeaningfulSentences.
//      mapValues(sentence => computeScore(sentence, positiveWords.value, negativeWords.value)).
//      filter { case (_, score) => score != 0 }
//
//  // Transform the (tweet, score) pair into a readable string and print it
//  textAndNonNeutralScore.map(makeReadable).print
//  textAndNonNeutralScore.saveAsTextFiles("/Users/ihebfehri/Desktop/test.txt")
//
//  // Now that the streaming is defined, start it
//  streamingContext.start()
//
//  // Let's await the stream to end - forever
//  streamingContext.awaitTermination()
//
//}
/**
  * A Spark Streaming application that receives tweets on certain
  * keywords from twitter datasource and find the popular hashtags
  *
  * Arguments: <comsumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <keyword_1> ... <keyword_n>
  * <comsumerKey>        - Twitter consumer key
  * <consumerSecret>     - Twitter consumer secret
  * <accessToken>        - Twitter access token
  * <accessTokenSecret>  - Twitter access token secret
  * <keyword_1>          - The keyword to filter tweets
  * <keyword_n>          - Any number of keywords to filter tweet
  */

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.storage.StorageLevel


/**
  * Command to launch on spark HDP
  *  /usr/hdp/current/spark-client/bin/spark-submit  --master yarn  --packages "org.apache.spark:spark-streaming-twitter_2.10:1.5.1" --class twitter.TwitterLiveFeed test2Stream.jar c82Cqj8hVUN5wg3VfKp6YoH2K n7NBQ61mtlJgOh4zPgLCeuUIfI18yHCjjVuGe2UeiQI6ZYEvYD 268885311-rzFGAVCzEYdxLQIDuNTbTTnTaT1lvx3inER3MweT g5pgpKkTYbAip6vdBfpXwPapAweLt6D70rL7i8Maxbl6i bigdata
  */

object TwitterLiveFeed {
  val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Streaming - PopularHashTags")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    sc.setLogLevel("ERROR")

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Set the Spark StreamingContext to create a DStream for every 25 seconds
    val ssc = new StreamingContext(sc, Seconds(25))
    // Pass the filter keywords as arguements

    //  val stream = FlumeUtils.createStream(ssc, args(0), args(1).toInt)
    val stream = TwitterUtils.createStream(ssc, None, filters)

    // Split the stream on space and extract hashtags
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // Get the top hashtags over the previous 60 sec window
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(50))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // Get the top hashtags over the previous 10 sec window
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(25))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // print tweets in the currect DStream
    stream.print()

    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 50 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 25 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

