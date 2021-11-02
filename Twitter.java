import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.*;

import scala.Function2;
import scala.Tuple2;
import twitter4j.Status;

import org.spark_project.guava.io.Files;

public class Twitter {
	
	private int totalCharacters;
	private int totalWords;
	
	private static int averageWords;
	private int averageCharacters;
	
	private static final Pattern SPACE = Pattern.compile(" ");
	
	private static void test() throws Exception {
		//Set up 
		SparkConf config = new SparkConf().setMaster("local[2]").setAppName("Assignmnet5");
		JavaStreamingContext jsscs = new JavaStreamingContext(config,new Duration(1000));
		Logger.getRootLogger().setLevel(Level.ERROR);
		jsscs.checkpoint(Files.createTempDir().getAbsolutePath());
		
		//Twitter Login
		System.setProperty("twitter4j.oauth.consumerKey", "Your Consumer Key");
		System.setProperty("twitter4j.oauth.consumerSecret", "Your Consumer Secert Key");
		System.setProperty("twitter4j.oauth.accessToken", "Your Access Token Key");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "Your Access Token Secert Key");
	
		//Getting tweets 
		JavaDStream<Status> tweets = TwitterUtils.createStream(jsscs);
		
		//Getting information from tweets
		JavaDStream<String> statuses = tweets.map(
			      new Function<Status, String>() {
			        public String call(Status status) { return status.getText(); }
			      }
			    );
		
		
		//printing tweets
		statuses.print();
		
		//Can add code to store tweet informatino if required
		
		//jsscs.checkpoint();
		jsscs.start();
		jsscs.awaitTermination();
		
		//The following code could also be utilised in a standalone manner to analyse tweets or text data
		
		// Add code to load tweets from file or database if required
		
		//Getting characters from tweets
		JavaDStream<String> characters = statuses.flatMap(
			     new FlatMapFunction<String, String>() {
			       public Iterator<String> call(String in) {
			         return Arrays.asList(in.split("")).iterator();
			       }
			     }
			   );
		
		//Getting numbers of characters
		JavaDStream<Long> numCharacters = characters.count();
	
		//Getting the words in the tweets
	    JavaDStream<String> words = statuses.flatMap(
			     new FlatMapFunction<String, String>() {
			       public Iterator<String> call(String s) {
			         return Arrays.asList(SPACE.split(s)).iterator();
			       }
			     }
			   );
	    
	    //Getting number of words
	    JavaDStream<Long> numWords = words.count();
	    
	    //Getting hashtags from tweets
		JavaDStream<String> hashTags = words.filter(
			     new Function<String, Boolean>() {
			       public Boolean call(String word) { return word.startsWith("#"); }
			     }
			   );
		
		
		// Average number of characters in tweet
		Long totalCharacters = totalCharacters + numCharacters.foreachRDD( new Function<JavaRDD<Long>>(){
			public Long call(JavaRDD<Long> v1) throws Exception {
				return v1.first();
			}
		}); 
		
		//Getting average words in tweets
		Long totalWords = totalWords + numWords.foreachRDD( new Function<JavaRDD<Long>>(){
			public Long call(JavaRDD<Long> v1) throws Exception {
				return v1.first();
			}
		}); 
		
		int totalTweets=0;
		statuses.foreachRDD( s -> total += 1);
		
		Long averageCharacters = totalCharacters/totalTweets;
		Long averageWords = totalWords/totalTweets;
		
		// Q3 - b
		//getting top 10 hastags from tweets
		//Map hashTags to tuple pairs containing hashtag and 1 
		JavaPairDStream<String, Integer> Htuples = hashTags.map(
			      new PairFunction<String, String, Integer>() {
			        public Tuple2<String, Integer> call(String in) {
			          return new Tuple2<String, Integer>(in, 1);
			        }
			      }
			    );
		
		//Combining the hashtags that are the same to get total occurences of each hashtag
	    JavaPairDStream<String, Integer> Hcounts = Htuples.reduceByKey(
			      new Function<Integer, Integer>() {
			        public Integer call(Integer i1, Integer i2) { return i1 + i2; }
			      }
			    );
	    
	    //sorting the hashtag,occurences tuples
		JavaPairDStream<Integer, String> swappedCounts = Hcounts.map(
	     new PairFunction<Tuple2<String, Integer>, Integer, String>() {
	       public Tuple2<Integer, String> call(Tuple2<String, Integer> in) {
	         return in.swap();
	       }
	     }
	   );

	   JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transform(
	     new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
	       public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> in) throws Exception {
	         return in.sortByKey(false);
	       }
	     });

	   //printing out the top 10 hashtags
	   sortedCounts.foreach(
	     new Function<JavaPairRDD<Integer, String>, Void> () {
	       public Void call(JavaPairRDD<Integer, String> rdd) {
	         String out = "\nTop 10 hashtags:\n";
	         for (Tuple2<Integer, String> t: rdd.take(10)) {
	           out = out + t.toString() + "\n";
	         }
	         System.out.println(out);
	         return null;
	       }
	     }
	   );

	  
	   
	   Long totalCharacters = totalCharacters + numCharacters.windows(Durations.seconds(300), Durations.seconds(30)).foreachRDD( new Function<JavaRDD<Long>>(){
			public Long call(JavaRDD<Long> v1) throws Exception {
				return v1.first();
			}
		}); 
		
		Long totalWords = totalWords + numWords.windows(Durations.seconds(300), Durations.seconds(30)).foreachRDD( new Function<JavaRDD<Long>>(){
			public Long call(JavaRDD<Long> v1) throws Exception {
				return v1.first();
			}
		}); 
	   
	   // Getting top 10 hashtags for last 5 minutes - Note: same as above except done evey 30 seconds for 5 minutes
		
		//Only change to above here where counts are done repeatedly every 30 seconds for 5 minute windows 
		JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
		     new Function2<Integer, Integer, Integer>() {
		        public Integer call(Integer i1, Integer i2) { return i1 + i2; }
		      },
		      new Function2<Integer, Integer, Integer>() {
		        public Integer call(Integer i1, Integer i2) { return i1 - i2; }
		      },
		      new Duration(60 * 5 * 1000),
		      new Duration(30 * 1000)
		);
		
		JavaPairDStream<Integer, String> swappedCounts = counts.map(
			     new PairFunction<Tuple2<String, Integer>, Integer, String>() {
			       public Tuple2<Integer, String> call(Tuple2<String, Integer> in) {
			         return in.swap();
			       }
			     }
			   );

			   JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transform(
			     new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
			       public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> in) throws Exception {
			         return in.sortByKey(false);
			       }
			     });

			   sortedCounts.foreach(
			     new Function<JavaPairRDD<Integer, String>, Void> () {
			       public Void call(JavaPairRDD<Integer, String> rdd) {
			         String out = "\nTop 10 hashtags:\n";
			         for (Tuple2<Integer, String> t: rdd.take(10)) {
			           out = out + t.toString() + "\n";
			         }
			         System.out.println(out);
			         return null;
			       }
			     }
			   );
	}
	
	
	public static void main(String[] args) throws Exception {
		System.out.println("In main");
		test();
	}
}

