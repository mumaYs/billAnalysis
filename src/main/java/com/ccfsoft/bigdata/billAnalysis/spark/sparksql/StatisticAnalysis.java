package com.ccfsoft.bigdata.billAnalysis.spark.sparksql;

import com.ccfsoft.bigdata.utils.PropertyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class StatisticAnalysis {
    public SparkConf conf;
    
    /**
     * SparkSQL 统计分析
     * @param spark
     */
    public static void runSparkSQL(SparkSession spark) {
        
        // 1.话单总量统计
//        Dataset<Row> tmp = spark.sql("SELECT " +
//                "begin_date," +
//                "CASE WHEN own_phone >= '13627551000' AND own_phone <= '13627551499' THEN 'CM' " +
//                "WHEN own_phone >= '13055661000' AND own_phone <= '13055661499' THEN 'CU' " +
//                "WHEN own_phone >= '18154981000' AND own_phone <= '18154981499' THEN 'CT' " +
//                "END AS ko," +
//                "CASE WHEN other_phone >= '13627551000' AND other_phone <= '13627551499' THEN 'CM' " +
//                "WHEN other_phone >= '13055661000' AND other_phone <= '13055661499' THEN 'CU' " +
//                "WHEN other_phone >= '18154981000' AND other_phone <= '18154981499' THEN 'CT' END AS ki " +
//                "FROM BILL");
//        tmp.registerTempTable("tmp");
//        Dataset<Row> billAmount = spark.sql("SELECT (CASE WHEN A.KI IS NULL THEN B.KO ELSE A.KI END) OPERATOR," +
//                "(CASE WHEN A.KICOUNT IS NULL THEN 0  ELSE A.KICOUNT END) CALL_IN_AMOUNT," +
//                "(CASE WHEN B.KOCOUNT IS NULL THEN 0 ELSE B.KOCOUNT END) CALL_OUT_AMOUNT," +
//                "(CASE WHEN A.BEGIN_DATE IS NULL THEN B.BEGIN_DATE ELSE A.BEGIN_DATE END) TDATE " +
//                "FROM (SELECT COUNT(T.KI) KICOUNT,T.BEGIN_DATE,T.KI FROM TMP T GROUP BY T.BEGIN_DATE, T.KI) A " +
//                "FULL OUTER JOIN (SELECT COUNT(T.KO) KOCOUNT, T.BEGIN_DATE, T.KO FROM TMP T GROUP BY T.BEGIN_DATE, T.KO) B " +
//                "ON A.BEGIN_DATE = B.BEGIN_DATE AND A.KI = B.KO");
//
//        // 全用户通话分析
//        Dataset<Row> totalData1 = spark.sql("select own_phone p1,other_phone p2,from_unixtime(unix_timestamp(begin_time,'HH:mm:ss'),'HH:00:00') call_time,talk_time,begin_date from BILL");
//        Dataset<Row> totalData2 = spark.sql("select other_phone p1,own_phone p2,from_unixtime(unix_timestamp(begin_time,'HH:mm:ss'),'HH:00:00') call_time,talk_time,begin_date from BILL");
//        Dataset<Row> noRepeatData = totalData1.except(totalData2);
//        Dataset<Row> repeatData = totalData1.intersect(totalData2);
//        noRepeatData.registerTempTable("noRepeatData");
//        repeatData.registerTempTable("repeatData");
//
//        // 2.全用户通话频率
//        Dataset<Row> callRateTmp = spark.sql("select count(p1) frequency,call_time,begin_date from noRepeatData group by call_time,begin_date " +
//                "union all select count(p1)/2 frequency,call_time,begin_date from repeatData group by call_time,begin_date");
//        callRateTmp.registerTempTable("callRateTmp");
//        Dataset<Row> callRate = spark.sql("SELECT BEGIN_DATE CALL_DATE,CALL_TIME,SUM(FREQUENCY) CALL_RATE FROM CALLRATETMP GROUP BY CALL_TIME,BEGIN_DATE ORDER BY BEGIN_DATE");
//
//        // 3.全用户平均通话时长
//        Dataset<Row> callTimeTmp = spark.sql("select avg(talk_time) avg_time,call_time,begin_date from noRepeatData group by call_time,begin_date " +
//                "union all select avg(talk_time) avg_time,call_time,begin_date from repeatData group by call_time,begin_date");
//        callTimeTmp.registerTempTable("callTimeTmp");
//        Dataset<Row> callTime = spark.sql("SELECT BEGIN_DATE CALL_DATE,CALL_TIME,AVG(AVG_TIME) AVG_TALK_TIME FROM CALLTIMETMP GROUP BY CALL_TIME,BEGIN_DATE");

        // 4.个人通话频率
        Dataset<Row> personCallRate = spark.sql("SELECT T.TELEPHONE,T.CALL_DATE,T.CALL_TIME,COUNT(*) CALL_RATE FROM " +
                "(SELECT OWN_PHONE TELEPHONE, BEGIN_DATE CALL_DATE,FROM_UNIXTIME(UNIX_TIMESTAMP(BEGIN_TIME, 'HH:mm:ss'),'HH:00:00') CALL_TIME FROM BILL " +
                "UNION ALL SELECT OTHER_PHONE TELEPHONE, BEGIN_DATE CALL_DATE,FROM_UNIXTIME(UNIX_TIMESTAMP(BEGIN_TIME, 'HH:mm:ss'),'HH:00:00') CALL_TIME FROM BILL) T " +
                "GROUP BY T.TELEPHONE,T.CALL_DATE,T.CALL_TIME");
        personCallRate.registerTempTable("personCallRate");

        // 个人通话频率周表
        Dataset<Row> personCallRateWk = spark.sql("SELECT TELEPHONE, WEEKOFYEAR(CALL_DATE) + 1 CALL_WK, CALL_TIME, SUM(CALL_RATE) CALL_RATE FROM personCallRate GROUP BY TELEPHONE, WEEKOFYEAR(CALL_DATE) + 1, CALL_TIME");
        // 个人通话频率月表
        Dataset<Row> personCallRateMon = spark.sql("SELECT TELEPHONE, month(CALL_DATE) CALL_MON, CALL_TIME, SUM(CALL_RATE) CALL_RATE FROM personCallRate GROUP BY TELEPHONE, month(CALL_DATE), CALL_TIME");

        // 5.个人通话时长
        Dataset<Row> personTalkTime = spark.sql("SELECT T.TELEPHONE,T.CALL_DATE,T.CALL_TIME,AVG(T.TALK_TIME) AVG_TALK_TIME FROM " +
                "(SELECT OWN_PHONE TELEPHONE,BEGIN_DATE CALL_DATE,FROM_UNIXTIME(UNIX_TIMESTAMP(BEGIN_TIME,'HH:mm:ss'),'HH:00:00') CALL_TIME,TALK_TIME FROM BILL " +
                "UNION ALL SELECT OTHER_PHONE TELEPHONE,BEGIN_DATE CALL_DATE,FROM_UNIXTIME(UNIX_TIMESTAMP(BEGIN_TIME,'HH:mm:ss'),'HH:00:00') CALL_TIME,TALK_TIME FROM BILL) T " +
                "GROUP BY T.TELEPHONE,T.CALL_DATE,T.CALL_TIME");
        personTalkTime.registerTempTable("personTalkTime");

        // 个人通话时长周表
        Dataset<Row> personTalkTimeWk = spark.sql("SELECT TELEPHONE, WEEKOFYEAR(CALL_DATE) + 1 CALL_WK, CALL_TIME, AVG(AVG_TALK_TIME) AVG_TALK_TIME FROM PERSONTALKTIME GROUP BY TELEPHONE, WEEKOFYEAR(CALL_DATE) + 1, CALL_TIME");
        // 个人通话时长月表
        Dataset<Row> personTalkTimeMon = spark.sql("SELECT TELEPHONE, month(CALL_DATE) CALL_MON, CALL_TIME, AVG(AVG_TALK_TIME) AVG_TALK_TIME FROM PERSONTALKTIME GROUP BY TELEPHONE, month(CALL_DATE), CALL_TIME");

        // 6.基站分布
        Dataset<Row> station = spark.sql("SELECT OWN_STATION_ID, OWN_XQ, COUNT(*) TNUMBER,BEGIN_DATE TDATE FROM BILL GROUP BY OWN_STATION_ID, OWN_XQ, BEGIN_DATE");

//        dbWrite(billAmount, "BILL_AMOUNT");
//        dbWrite(callRate,"BILL_CALLRATE_FULLDATA");
//        dbWrite(callTime,"BILL_TALKTIME_FULLDATA");
        dbWrite(personCallRate,"BILL_CALLRATE_PERSONAL");
        dbWrite(personTalkTime,"BILL_TALKTIME_PERSONAL");
        dbWrite(station,"BILL_STATION_HEATMAP");

        // 增加个人通话表的周表及月表
        dbWrite(personCallRateWk,"BILL_CALLRATE_PERSONAL_WEEK");
        dbWrite(personCallRateMon,"BILL_CALLRATE_PERSONAL_MONTH");
        dbWrite(personTalkTimeWk,"BILL_TALKTIME_PERSONAL_WEEK");
        dbWrite(personTalkTimeMon,"BILL_TALKTIME_PERSONAL_MONTH");
    }
    
    /**
     * 将分析结果存入mysql
     * @param data            分析的数据
     * @param tableName       存入的表
     */
    public static void dbWrite(Dataset<Row> data, String tableName) {
//        String url = "jdbc:mysql://192.168.1.221:3306/blbillTest?useUnicode=true&characterEncoding=UTF-8";
//        Properties connectionProperties = new Properties();
//        connectionProperties.setProperty("user", "root");
//        connectionProperties.setProperty("password", "123456");

        /**
         * 用配置文件的方法
         */
        Properties connectionProperties = PropertyConstants.getProperties();

        // mysql
//        String url = connectionProperties.getProperty("url");
//        connectionProperties.getProperty("user");
//        connectionProperties.getProperty("password");

        // oracle
        String url = connectionProperties.getProperty("url");
        connectionProperties.getProperty("user");
        connectionProperties.getProperty("password");

        data.write().mode(SaveMode.Overwrite).jdbc(url, tableName, connectionProperties);
    }
}
