package structuredStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import static org.apache.spark.sql.functions.*;

public class exo {

    public static void main(String[] args) throws Exception {
        // Configuration de Spark
        SparkConf sparkConf = new SparkConf().setAppName("StreamingApp").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        // Lecture des données en streaming
        Dataset<Row> incidentsStream = spark
                .readStream()
                .option("header", "true")
                .csv("../../../incidents.csv");  // Remplacez par le chemin de vos fichiers CSV

        // 1. Afficher d’une manière continue le nombre d’incidents par service.
        Dataset<Row> incidentsByService = incidentsStream.groupBy("service").count();

        // Affichage en console
        StreamingQuery query1 = incidentsByService
                .writeStream()
                .outputMode("complete")  // "complete" pour afficher le résultat complet à chaque batch
                .format("console")
                .start();

        // 2. Afficher d’une manière continue les deux années où il y avait plus d’incidents.
        Dataset<Row> incidentsByYear = incidentsStream
                .withColumn("year", year(col("date")))
                .groupBy("year")
                .count()
                .orderBy(desc("count"))
                .limit(2);

        // Affichage en console
        StreamingQuery query2 = incidentsByYear
                .writeStream()
                .outputMode("complete")  // "complete" pour afficher le résultat complet à chaque batch
                .format("console")
                .start();

        // Attente que la requête se termine
        query1.awaitTermination();
        query2.awaitTermination();
    }
}
