import com.ccfsoft.bigdata.billAnalysis.arangodb.RelationNetworkAnalyze;
import com.ccfsoft.bigdata.billAnalysis.spark.sparksql.DataTransfer;
import com.ccfsoft.bigdata.billAnalysis.spark.sparksql.StatisticAnalysis;
import com.ccfsoft.bigdata.utils.PropertyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 初始化程序
 * 实现多个spark任务的调度
 */
public class Init {
    public static void main(String[] args) {
        // 加载Spark配置
        SparkConf conf = new SparkConf()
                .setAppName("Bill Analysis For Bei lun")
                .setMaster("yarn-cluster")
                .set("es.nodes", PropertyConstants.getPropertiesKey("es.nodes"))
                .set("es.port", PropertyConstants.getPropertiesKey("es.port"));

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        //begin_date|begin_time|call_type|other_city|other_location|other_phone|own_city|own_location|own_phone|own_station_id|talk_time|
        Dataset<Row> df = spark.read().json(PropertyConstants.getPropertiesKey("hdfs") + "/00DATA/00LOCAL/02BILL/BeiLun2017.txt");
        df.createOrReplaceTempView("BILL");

//        //1.sparksql统计分析
//        StatisticAnalysis.runSparkSQL(spark);
//
//        //2.关系网络分析(全量话单)
//        BillGraphX.billGraphX(spark);
//
//        //3.关系网络入库(ArangoDB)
//        RelationNetworkAnalyze.relationNetworkAnalyze(spark);

        //4.话单数据入ElasticSearch
        DataTransfer.copyDataToES(spark);

        spark.stop();
    }
}
