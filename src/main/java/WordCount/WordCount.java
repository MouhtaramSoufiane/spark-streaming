package WordCount;


import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("SPARK STREAMS").setMaster("local[*]").set("spark.ui.port", "4040")
                .set("spark.port.maxRetries", "100");
        JavaStreamingContext streamingContext=new JavaStreamingContext(conf, Durations.seconds(9));
        JavaReceiverInputDStream<String> dStreamLines = streamingContext.socketTextStream("localhost", 8888);

        JavaDStream<String> javaDStream = dStreamLines.flatMap(l -> Arrays.asList(l.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairDStream = javaDStream.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> reduceByKey = pairDStream.reduceByKey((a, b) -> a + b);
        reduceByKey.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
