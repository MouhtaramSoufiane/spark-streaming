package sparkStreaming;


import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;

import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;


import java.util.Arrays;

public class SparkStreamsApp2 {
    public static void main(String[] args) throws Exception {
        SparkConf config=new SparkConf().setAppName("SPARK STREAM 1").setMaster("local[*]").set("spark.ui.port", "4040")
                .set("spark.port.maxRetries", "100");
        JavaStreamingContext sc=new JavaStreamingContext(config, Durations.seconds(8));
        JavaDStream<String> fileStream = sc.textFileStream("http://localhost:50070/explorer.html#/names");
        JavaDStream<String> dbStramWords = fileStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> mapToPair = dbStramWords.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> reduceByKey = mapToPair.reduceByKey((a, b) -> a + b);

        reduceByKey.print();

        sc.start();

        sc.awaitTermination();
    }
}
