import com.alibaba.fastjson.JSON;
import com.ccfsoft.bigdata.utils.PropertyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SparkTest {
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

        Dataset<Row> df = spark.read().json("E:\\TMP\\11.json");
        df.schema();
        df.show();
//        // spark读取json数组解析成Dataset<Row>
//        JavaRDD<Bill> dataRDD =spark.read()
//                .textFile("E:\\TMP\\123.json")
//                .javaRDD()
//                .map(line -> JSON.parseObject(line,Data.class).getData().p);
//
//        Dataset<Row> ds = spark.createDataFrame(dataRDD, Bill.class);


        spark.stop();
    }
}
