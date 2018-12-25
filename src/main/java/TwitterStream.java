import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import scala.Tuple3;
import twitter4j.Status;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TwitterStream {

    public static void main(String[] args) throws InterruptedException {

//        Set the configuration required the Spark
//        1. AppName - Name of the Application
//        2. Master - host name of the Spark Master (local for local mode) and number of cores

        SparkConf conf = new SparkConf()
                .setMaster("local[3]")
                .setAppName("SparkTwitterHelloWorldExample");

//        Setup JavaStreamingContext using above spark configuration and the batch interval is set to 1 second
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

//        Spark Logging level is set to ERROR to avoid all the INFO messages in the console.
//        NOTE: This is optional and is done to clearly see the output of this code. This
//        should not be done in production environment.
        jssc.sparkContext().setLogLevel("ERROR");

//        Create a DStream using TwitterUtils
//        OAuth credentials for twitter4j are provided in twitter4j.properties file in resources folder of
//        maven project.
        JavaDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

//        Question 1
//        ----------
//        Extract the text from tweet and print first 10 tweets and create a new DStream with
//        text from each tweet
        JavaDStream<String> tweets = twitterStream.map(Status::getText);
        tweets.print();

//        Question 2
//        ----------
//        Below code creates a new DStream of type Tuple3 with three elements by mapping each tweet
//        into following:
//        1. Tweet text as element 1
//        2. Count of characters in the tweet as element2
//        3. Count of words in the tweet as element3
        JavaDStream<Tuple3<String, Integer, Integer>> tweetTotals =
                tweets
                    .map(t -> new Tuple3<>(t, t.length(), t.split(" ").length));


//        Extract hashtags from tweets and create a new DStream as follows:
//        1. All the words from each tweet are extracted using a regular expression by idenfying either a space
//           or '.' or ',' or new line or tab.
//        2. All the hashtags can be then filtered out by checking for words which starts with #
//        3. Remove the character.
        JavaDStream<String> hashtags = tweets.flatMap(t -> {
            List<String> words = Arrays.asList(t.split("[ .,:\\n\\t]"));
            return words.stream()
                    .filter(w -> w.startsWith("#"))
                    .map(w -> w.replace("#", ""))
                    .collect(Collectors.toList()).iterator();
        });


//        Print first 10 tweets, count of characters and words in a tweet
        tweetTotals.print();

//        Print first 10 hashtags from all the tweets in current batch
        hashtags.print();

//        Question 3a
//        -----------

//        Compute the Average Characters in the batch by calling the mean function for using total characters in each
//        tweet (this is available in tweetTotals DStream)

        tweetTotals.foreachRDD(t -> {
            if (t.count() > 0) {
                double avgChars = t.mapToDouble(tc -> tc._2()).mean();
                System.out.println("Average Characters : " + avgChars);
            }
        });

//        Compute the Average Words in the batch by calling the mean function for using total words in each
//        tweet (this is available in tweetTotals DStream)

        tweetTotals.foreachRDD(t -> {
            if (t.count() > 0) {
                double avgWords = t.mapToDouble(tc -> tc._3()).mean();
                System.out.println("Average Words : " + avgWords);
            }
        });

//        Question 3b
//        -----------
//        Count the top 10 hashtags is done by
//        1. getting the counts of hashtags as a pair (hashtag, count)
//        2. swapping the pair from (hashtag, count) to (count, hashtag)
//        3. sorting the pairs by Key
//        4. Swapping the pairs back to (hashtag, count) format.
        JavaPairDStream<String, Long> hashtagsCountsSorted =
                hashtags.countByValue()
                        .mapToPair(Tuple2::swap)
                        .transformToPair(hcswap -> hcswap.sortByKey(false))
                        .mapToPair(Tuple2::swap);

//      Print top 10 hashtags.
        hashtagsCountsSorted.print(10);


//        Question 3c
//        -----------

//        Compute the average number of characters for tweets in last 5 minutes every 30 seconds
//        This is done by setting the window size to a Duration of 5 minutes and calculation time as
//        30 seconds

        tweetTotals
                .window(Durations.minutes(5), Durations.seconds(30))
                .foreachRDD(t -> {
                    if (t.count() > 0) {
                        double avgChars = t.mapToDouble(tc -> tc._2()).mean();
                        System.out.println("Windowed Average Characters : " + avgChars);
                    }
        });

//        Compute the average number of words for tweets in last 5 minutes every 30 seconds
//        This is done by setting the window size to a Duration of 5 minutes and calculation time as
//        30 seconds

        tweetTotals
                .window(Durations.minutes(5), Durations.seconds(30))
                .foreachRDD(t -> {
                    if (t.count() > 0) {
                        double avgWords = t.mapToDouble(tc -> tc._3()).mean();
                        System.out.println("Windowed Average Words : " + avgWords);
                    }
        });


//        Compute the top 10 hashtags in last 5 minutes every 30 seconds.
//        A new DStream is created with the hashtags in last 5 minutes using the window operation.
//        This is used to compute the top 10 hashtags every 30 seconds.

        JavaDStream<String> hashtagsWindowed = hashtags
                .window(Durations.minutes(5), Durations.seconds(30));

        JavaPairDStream<String, Long> hashtagsCountsSortedWindow =
                hashtagsWindowed.countByValue()
                        .mapToPair(Tuple2::swap)
                        .transformToPair(hcswap -> hcswap.sortByKey(false))
                        .mapToPair(Tuple2::swap);

        hashtagsCountsSortedWindow.print(10);

//        Start the stream processing
        jssc.start();
//        Run the stream processing until manually terminated.
        jssc.awaitTermination();
    }
}
