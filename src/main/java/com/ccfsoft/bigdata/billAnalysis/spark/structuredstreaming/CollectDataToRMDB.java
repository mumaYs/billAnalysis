package com.ccfsoft.bigdata.billAnalysis.spark.structuredstreaming;

import com.ccfsoft.bigdata.billAnalysis.spark.entity.BaseStation;
import com.ccfsoft.bigdata.billAnalysis.spark.sparksql.StatisticAnalysis;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * 收集数据到关系型数据库
 */
public class CollectDataToRMDB {

    /**
     * 状态流数据导入关系型数据库
     * @param spark
     */
    public static void process(SparkSession spark) throws Exception{
        // Subscribe to 1 topic defaults to the earliest and latest offsets
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.1.202:9092,192.168.1.207:9092,192.168.1.208:9092")
                .option("subscribe", "RMDBTopic")
                .option("includeTimestamp", true)
                .load()
                .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)");
        /**
         *
         |-- key: binary (nullable = true)
         |-- value: binary (nullable = true)
         |-- topic: string (nullable = true)
         |-- partition: integer (nullable = true)
         |-- offset: long (nullable = true)
         |-- timestamp: timestamp (nullable = true)
         |-- timestampType: integer (nullable = true)
         */
        lines.printSchema();
//
//        // Split the lines into words, retaining timestamps
//        Dataset<Row> words = lines
//                .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
//                .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
//                            List<Tuple2<String, Timestamp>> result = new ArrayList<>();
//                            for (String word : t._1.split(" ")) {
//                                result.add(new Tuple2<>(word, t._2));
//                            }
//                            return result.iterator();
//                        },
//                        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
//                ).toDF("word", "timestamp");
//
//        // Group the data by window and word and compute the count of each group
//        String windowDuration = 10 + " seconds";
//        String slideDuration = 5 + " seconds";
//        Dataset<Row> windowedCounts = words
//                .withWatermark("timestamp", "10 minutes")
//                .groupBy(
//                        functions.window(words.col("timestamp"), windowDuration, slideDuration),
//                        words.col("word")
//                ).count().orderBy("window");
//
//        // Start running the query that prints the windowed word counts to the console
//        StreamingQuery query = windowedCounts.writeStream()
//                .outputMode("complete")
//                .format("console")
//                .option("truncate", "false")
//                .start();
//
//        query.awaitTermination();
    }
}
