import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

public class MyKMeansClass {
	
	public static void main(String[] args) {
		//Set up and getting data 
		System.setProperty("hadoop.home.dir", "C:/winutils");
		SparkConf conf = new SparkConf().setAppName("TweetGroupingOnLocation").setMaster("local[4]").set("spark.executor.memory", "2g");;
		JavaSparkContext sc = new JavaSparkContext(conf);
		String path = "Path to data";
		//Might need to change depending on how data is stored in this example data was stored in txt file
		JavaRDD<String> data = sc.textFile(path);
		
		//converting the input data into vectors to create the cluster
		//once again might need to adjust depending on how data was stored
		JavaRDD<Vector> parsedData = data.map(s -> {
			String[] sarray = s.split(",");
			double[] values = new double[2];
			for (int i = 0; i < values.length; i++)
				values[i] = Double.parseDouble(sarray[i]);
			return Vectors.dense(values);

		});
		//caching the changed input data
		parsedData.cache();
		
		//setting desired number of clusters and Itterations
		int numClusters = 4;
		int numIterations = 20;
		
		//Creating the K-Means model
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
		
		//Creating pairs containing the tweets and the cluster they belong to
		JavaPairRDD<Object, Object> predictionAndLabels = data.mapToPair(s -> {
			String[] testArray = s.split(",");
			double[] testValues = new double[2];
			for (int i = 0; i < testValues.length; i++)
				testValues[i] = Double.parseDouble(testArray[i]);
			Tuple2 t = new Tuple2<>(clusters.predict(Vectors.dense(testValues)), testArray[testArray.length-1]);
			return t;
		});
		
		//printing out the tweet cluster pairs
		predictionAndLabels.groupByKey().foreach(p -> System.out.println("Tweet " + p._2 + " is in cluster " + p._1));
		
		sc.stop();
		sc.close();	
	}

}
