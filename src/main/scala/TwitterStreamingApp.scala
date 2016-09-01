import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import twitter4j.internal.json.StatusJSONImpl
import java.util.Date
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
/**
 * 
 * Use this as starting point for performing Tsentiment analysis on tweets from Twitter
 *
 *  To run this app
 *   sbt package
 *   $SPARK_HOME/bin/spark-submit --class "TwitterStreamingApp" --master local[*] ./target/scala-2.10/twitter-streaming-assembly-1.0.jar
 *      <consumer key> <consumer secret> <access token> <access token secret>
 */
object TwitterStreamingApp {
  def main(args: Array[String]) {
    
    if (args.length < 4) {
      System.err.println("Usage: TwitterStreamingApp <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

  	val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    println("Twitter Sentiment Analysis!! Initializing..");
    
    //val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    //to run this application inside an IDE, comment out previous line and uncomment line below and vice-versa
    
    val sparkConf = new SparkConf().setAppName("TwitterSentimentAnalysis").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
  	val sc = ssc.sparkContext
    val stream = TwitterUtils.createStream(ssc, None, filters)
    
    //get stop words, positive words and negative words and collect them to form a set 
    val stopWords = sc.textFile("src/main/resources/stop-words.txt").collect().toSet
    val posWords = sc.textFile("src/main/resources/pos-words.txt").collect().toSet
    val negWords = sc.textFile("src/main/resources/neg-words.txt").collect().toSet
    
    //get user language and extract only english ones, remove all punctuation marks and finally filter empty tweets 
    val filteredTweet = stream.filter(tweet => "en".equals(tweet.getUser.getLang))
                               .map(tweet => tweet.getText.replaceAll("[^a-zA-Z\\s]", "").trim)
                               .filter(tweet => tweet.length > 0)

    //tokenize tweet and remove stop words 
    val tokenizedTweet = filteredTweet.map(tweet => {  
                          var getTweet = ""
                          val tokens = tweet.split(" ")
                          for(word <- tokens) {
                            if(!stopWords.contains(word)) {
                                getTweet += word + " "
                            }
                          }
                          getTweet
                        })
                        
    //compute number of positive and negative words in the filtered tweet
    val getSentimentCount = (tokens: Array[String], comparisonSet: Set[String]) => {
      var count = 0
      for(word <- tokens) {
        if(comparisonSet.contains(word)) {
            count += 1
        }
      }
      count
    }
                        
     //calculate tweet score 
    val getTweetScore = (input: RDD[(String)]) => {
      input.map(tweet => {
        val tweetToken = tweet.split(" ")
        val countPositive = getSentimentCount(tweetToken, posWords)
        val countnegative = getSentimentCount(tweetToken, negWords)
        val resScore = countPositive - countnegative
          if (resScore < 0)       (tweet, 0, 1, 0)
          else if (resScore > 0)  (tweet, 1, 0, 0)
          else                    (tweet, 0, 0, 1)
      })
    }

    //get tweet and score 
    val getTweetAndScore = tokenizedTweet.transform(getTweetScore)
  
    //print tweet and score, taking 10 every time
    getTweetAndScore.foreachRDD(rdd => {
      val list = rdd.take(10)
        list.foreach(tweet => {
        println( tweet._1 + "--\t" + "Positive:" + tweet._2 + "\t" + "Negative:" + tweet._3 + "\t" + "Neutral:" + tweet._4)
      })
    })

    //combine scores
    val combineScores = (s: (Int, Int, Int), t: (Int, Int, Int)) => {
      (s._1 + t._1, s._2 + t._2, s._3 + t._3)
   }
    
    //count # of negative, positive and neutral count, print in every 10 seconds and 10 seconds sliding window
    val sentimentCount10 = getTweetAndScore.map(t => (t._2, t._3, t._4)).
                                            reduceByWindow(combineScores, Seconds(10), Seconds(10))

    //print sentiment count of 10 seconds
    sentimentCount10.foreachRDD(rdd => {
                     val list = rdd.take(1)
                     println("\nSentiment Count in 10 seconds : \n")
                     list.foreach{case(positive, negative, neutral) => 
                                  println("Positive: " + positive + " Negative:" + negative + " Neutral:" + neutral) }
                     })

    //count # of negative, positive and neutral count, print in every 30 seconds and 10 seconds sliding window
    val sentimentCount30 = getTweetAndScore.map(t => (t._2, t._3, t._4)).
                                            reduceByWindow(combineScores, Seconds(30), Seconds(10))

    //print sentiment count of 30 seconds
    sentimentCount30.foreachRDD(rdd => {
                val list = rdd.take(1)
                println("\nSentiment Count in 30 seconds : \n")
                list.foreach{case(positive, negative, neutral) =>
                             println("Positive: " + positive + " Negative:" + negative + " Neutral:" + neutral) }
                })
      
    
    ssc.start()
    ssc.awaitTermination()
  }
}