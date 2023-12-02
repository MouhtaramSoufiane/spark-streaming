package structuredStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class StructuredStreaming {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession session= SparkSession.builder()
                .appName("Structured Streaming")
                .master("local[*]")
                .config("spark.ui.port", "4137")
                .config("spark.port.maxRetries", "100")
                .getOrCreate();

        Dataset<Row> inputTable=session.readStream()
                .format("socket")
                .option("host","localhost")
                .option("port",8888)
                .load();
        StreamingQuery query = inputTable.writeStream().format("console").outputMode("append").start();
        query.awaitTermination();
    }
}
