import com.ccfsoft.bigdata.billAnalysis.spark.sparksql.CellDataToOracle;
import com.ccfsoft.bigdata.billAnalysis.spark.sparksql.DataTransfer;
import com.ccfsoft.bigdata.utils.PropertyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.mortbay.util.ajax.JSON;


public class InitTest {
    public static void main(String[] args) {
        // 加载Spark配置
        SparkConf conf = new SparkConf()
                .setAppName("Bill Analysis For Bei lun")
                .setMaster("local")
                .set("es.nodes", PropertyConstants.getPropertiesKey("es.nodes"))
                .set("es.port", PropertyConstants.getPropertiesKey("es.port"));
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

//        begin_date|begin_time|call_type|other_city|other_location|other_phone|own_city|own_location|own_phone|own_station_id|talk_time|
//        Dataset<Row> df = spark.read().json("E:\\TMP\\BeiLun2017*");
//        df.createOrReplaceTempView("BILL");

        //1.sparksql统计分析
//        StatisticAnalysis.runSparkSQL(spark);

        //2.关系网络分析(全量话单)
//        BillGraphX.billGraphX(spark);

        //3.关系网络入库(ArangoDB)
//        RelationNetworkAnalyze.relationNetworkAnalyze(spark);

//        //4.话单数据入ElasticSearch
//        Dataset<Row> baseStation = spark.read().json("E:\\TMP\\JIZHAN.txt");
//        baseStation.createOrReplaceTempView("BASESTATION");
//        // 关联基站经纬度
//        Dataset<Row> bills = spark.sql("SELECT T1.own_phone,'22.39265' station_lat,'113.97579' station_lon," +
//                "concat(T1.begin_date,' ',T1.begin_time) date_time,CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(concat(T1.begin_date,' ',T1.begin_time),'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHHmmss') AS BIGINT) timestamp," +
//                "CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(concat(T1.begin_date,' ',T1.begin_time),'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHHmmss') AS string) str_timestamp FROM BILL T1 " +
//                "LEFT OUTER JOIN BASESTATION T2 on T1.own_station_id=T2.station_id");
////        bills.show();
////        Dataset<Row> bills = spark.sql("SELECT T1.own_phone,T2.station_lat,T2.station_lon,T1.begin_date,T1.begin_time " +
////                "FROM BILL T1 LEFT OUTER JOIN BASESTATION T2 on T1.own_station_id=T2.station_id");
//        //话单数据存入ES
//        JavaEsSparkSQL.saveToEs(bills, "data/Bill");
        //5.基站数据入Oracle
        CellDataToOracle.copyDataToOracle(spark,"F:\\data\\cellinfo_test.txt");

        spark.stop();
    }
}
