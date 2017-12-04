package com.ccfsoft.bigdata.billAnalysis.spark.sparksql;


import com.ccfsoft.bigdata.billAnalysis.spark.entity.BaseStation;
import com.ccfsoft.bigdata.utils.PropertyConstants;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

/**
 * 数据转移类
 */
public class DataTransfer {

    /**
     * 将数据从HDFS存入ES
     * @param spark
     */
    public static void copyDataToES(SparkSession spark) {
        // 基站数据：
        JavaRDD<BaseStation> baseStationRDD = spark.read()
                .textFile(PropertyConstants.getPropertiesKey("hdfs") + "/00DATA/01OUT/00BASE_STATION/*")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split("\t");
                    BaseStation baseStation = new BaseStation();
                    baseStation.setMcc(parts[0]);
                    baseStation.setMnc(parts[1]);
                    baseStation.setLac(parts[2]);
                    baseStation.setCi(parts[3]);
                    baseStation.setLat(parts[4]);
                    baseStation.setLon(parts[5]);
                    baseStation.setAcc(parts[6]);
                    baseStation.setDate(parts[7]);
                    baseStation.setX(parts[8]);
                    baseStation.setAddr(parts[9]);
                    baseStation.setProvince(parts[10]);
                    baseStation.setCity(parts[11]);
                    baseStation.setDistrict(parts[12]);
                    baseStation.setTownship(parts[13]);
                    return baseStation;
                });

        Dataset<Row> baseStationDF = spark.createDataFrame(baseStationRDD, BaseStation.class);
        baseStationDF.createOrReplaceTempView("BASESTATION");
        // 关联基站经纬度
        Dataset<Row> bills = spark.sql("SELECT \n" +
                "  NVL (T1.call_type, '') call_type,\n" +
                "  NVL (T1.other_city, '') other_city,\n" +
                "  NVL (T1.other_location, '') other_location,\n" +
                "  NVL (T1.other_phone, '') other_phone,\n" +
                "  CASE WHEN find_in_set(substr(trim(T1.other_phone),0,2), '134,135,136,137,138,139,147,150,151,152,157,158,159,178,182,183,184,187,188') > 0 THEN '移动' \n" +
                "  WHEN find_in_set(substr(trim(T1.other_phone),0,2), '130,131,132,155,156,185,186,145,176') > 0 THEN '联通' \n" +
                "  WHEN find_in_set(substr(trim(T1.other_phone),0,2), '133,153,177,180,181,189,173,177') > 0 THEN '电信' \n" +
                "  ELSE '未知' END other_operator,\n" +
                "  NVL (T1.own_city, '') own_city,\n" +
                "  NVL (T1.own_location, '') own_location,\n" +
                "  NVL (T1.own_phone, '') own_phone,\n" +
                "  CASE WHEN find_in_set(substr(trim(T1.own_phone),0,2), '134,135,136,137,138,139,147,150,151,152,157,158,159,178,182,183,184,187,188') > 0 THEN '移动' \n" +
                "  WHEN find_in_set(substr(trim(T1.own_phone),0,2), '130,131,132,155,156,185,186,145,176') > 0 THEN '联通' \n" +
                "  WHEN find_in_set(substr(trim(T1.own_phone),0,2), '133,153,177,180,181,189,173,177') > 0 THEN '电信' \n" +
                "  ELSE '未知' END own_operator,\n" +
                "  NVL (T1.own_station_id, '') own_station_id,\n" +
                "  NVL (T1.talk_time, '') talk_time,\n" +
                "  NVL (T1.own_xq, '') own_xq,\n" +
                "  NVL (T1.other_xq, '') other_xq,\n" +
                "  NVL (T1.three_phone, '') three_phone,\n" +
                "  NVL (T1.jhj_id, '') jhj_id,\n" +
                "  NVL (T1.other_phone_qz, '') other_phone_qz,\n" +
                "  NVL (T1.jswzq, '') jswzq,\n" +
                "  NVL (T1.jsxq, '') jsxq,\n" +
                "  NVL (T1.own_thd, '') own_thd,\n" +
                "  NVL (T1.other_station_id, '') other_station_id,\n" +
                "  CONCAT(T1.begin_date, ' ', T1.begin_time) date_time,\n" +
                "  CAST(\n" +
                "    FROM_UNIXTIME(\n" +
                "      UNIX_TIMESTAMP(\n" +
                "        CONCAT(T1.begin_date, ' ', T1.begin_time),\n" +
                "        'yyyy-MM-dd HH:mm:ss'\n" +
                "      ),\n" +
                "      'yyyyMMddHHmmss'\n" +
                "    ) AS BIGINT\n" +
                "  ) timestamp,\n" +
                "  CAST(\n" +
                "    FROM_UNIXTIME(\n" +
                "      UNIX_TIMESTAMP(\n" +
                "        CONCAT(T1.begin_date, ' ', T1.begin_time),\n" +
                "        'yyyy-MM-dd HH:mm:ss'\n" +
                "      ),\n" +
                "      'yyyyMMddHHmmss'\n" +
                "    ) AS STRING\n" +
                "  ) str_timestamp,\n" +
                "  NVL (T2.mcc, '') mcc,\n" +
                "  NVL (T2.mnc, '') mnc,\n" +
                "  NVL (T2.lac, '') lac,\n" +
                "  NVL (T2.ci, '') ci,\n" +
                "  NVL (T2.lat, '') lat,\n" +
                "  NVL (T2.lon, '') lon,\n" +
                "  NVL (T2.acc, '') acc,\n" +
                "  NVL (T2.date, '') date,\n" +
                "  NVL (T2.x, '') x,\n" +
                "  NVL (T2.addr, '') addr,\n" +
                "  NVL (T2.province, '') province,\n" +
                "  NVL (T2.city, '') city,\n" +
                "  NVL (T2.district, '') district,\n" +
                "  NVL (T2.township, '') township \n" +
                "FROM\n" +
                "  BILL T1 \n" +
                "  LEFT OUTER JOIN BASESTATION T2 \n" +
                "    ON (\n" +
                "      T1.own_station_id = T2.lac \n" +
                "      AND T1.own_xq = T2.ci\n" +
                "    )");


        //话单数据存入ES
        JavaEsSparkSQL.saveToEs(bills, "data/bill");
    }
}
