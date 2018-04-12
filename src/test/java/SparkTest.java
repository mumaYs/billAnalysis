import com.alibaba.fastjson.JSON;
import com.ccfsoft.bigdata.billAnalysis.spark.entity.BaseStation;
import com.ccfsoft.bigdata.billAnalysis.spark.entity.TestEntity;
import com.ccfsoft.bigdata.billAnalysis.spark.sparksql.StatisticAnalysis;
import com.ccfsoft.bigdata.utils.PropertyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SparkTest {
    public static void main(String[] args) throws Exception {
        // 加载Spark配置
        SparkConf conf = new SparkConf()
                .setAppName("Test")
                .setMaster("local[2]");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

//        Dataset<Row> df = spark.read().json("E:\\TMP\\11.json");
//        df.schema();
//        df.show();
//        // spark读取json数组解析成Dataset<Row>
//        JavaRDD<Bill> dataRDD =spark.read()
//                .textFile("E:\\TMP\\123.json")
//                .javaRDD()
//                .map(line -> JSON.parseObject(line,Data.class).getData().p);
//
//        Dataset<Row> ds = spark.createDataFrame(dataRDD, Bill.class);

//        /**
//         * 测试structured-streaming的数据导入功能
//         * @deprecated structured-streaming暂不支持rdbms
//         */
//        Dataset<String> kafkaJson = spark
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "192.168.1.202:9092,192.168.1.207:9092,192.168.1.208:9092")
//                .option("subscribe", "RMDBTopic")
//                .load()
//                .selectExpr("CAST(value AS STRING)")
//                .as(Encoders.STRING());
//
//        Dataset<Row> testDF = spark.createDataFrame(kafkaJson.javaRDD().map(line -> JSON.parseObject(line,TestEntity.class)), TestEntity.class);
//
//        //基站数据存入oracle
//        StatisticAnalysis.dbWrite(testDF,"Test");
//
//        StreamingQuery query = kafkaJson
//                .writeStream()
//                .format("console")
//                .start();
//
//        query.awaitTermination();


        spark.stop();
    }
}
