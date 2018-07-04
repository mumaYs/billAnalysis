import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 行转列，列转行实例
 */
public class RowAndCol {
    public static void main(String[] args) throws Exception {
        // 加载Spark配置
        SparkConf conf = new SparkConf()
                .setAppName("Test")
                .setMaster("local[2]");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        /**
         * Pivot（多行转多列）功能测试，spark暂不支持unpivot,但可以通过casewhen和union较烦实现功能
         */
        /*
        原始数据：
        +---+---+-----+---+
        |  A|  B|    C|  D|
        +---+---+-----+---+
        |foo|one|small|  1|
        |foo|one|large|  2|
        |foo|one|large|  2|
        |foo|two|small|  3|
        |foo|two|small|  3|
        |bar|one|large|  4|
        |bar|one|small|  5|
        |bar|two|small|  6|
        |bar|two|large|  7|
        +---+---+-----+---+
         */
        Dataset<Row> df = spark.read().json("/Users/yangsu/Downloads/TMP/tmp2.txt");

        /*
        行转列：
        +---+---+-----+-----+
        |  A|  B|large|small|
        +---+---+-----+-----+
        |foo|one|    4|    1|
        |foo|two| null|    6|
        |bar|two|    7|    6|
        |bar|one|    4|    5|
        +---+---+-----+-----+
         */
//        df.groupBy("A","B").pivot("C").sum("D").printSchema();

        /**
         * 多行转单列
         */
        /*
        +---+---+--------------+
        |  A|  B|collect_set(C)|
        +---+---+--------------+
        |foo|one|[large, small]|
        |foo|two|       [small]|
        |bar|two|[large, small]|
        |bar|one|[large, small]|
        +---+---+--------------+
         */
//        df.groupBy("A","B").agg(collect_set("C")).show();


        /*
            +---+---+-----------+------+
            |  A|  B|      sizes|sum(D)|
            +---+---+-----------+------+
            |foo|one|large,small|     5|
            |foo|two|      small|     6|
            |bar|two|large,small|    13|
            |bar|one|large,small|     9|
            +---+---+-----------+------+
         */
        df.registerTempTable("test");
        spark.sql("select t.A, t.B, concat_ws(',',collect_set(t.C)) as sizes, sum(t.D)  from test t group by t.A,t.B").registerTempTable("test2");

        /**
         * 单列转多行
         */
        spark.sql("select A,B,size from test2 t lateral view explode(split(sizes,',')) as size").show();

        spark.stop();
    }
}
