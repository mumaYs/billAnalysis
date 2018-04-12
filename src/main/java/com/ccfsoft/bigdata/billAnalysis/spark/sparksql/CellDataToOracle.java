package com.ccfsoft.bigdata.billAnalysis.spark.sparksql;


import com.ccfsoft.bigdata.billAnalysis.spark.entity.BaseStation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 数据转移类
 */
public class CellDataToOracle {

    /**
     * 将数据从HDFS存入Ora
     * @param spark
     */
    public static void copyDataToOracle(SparkSession spark,String path) {
        // 基站数据：
        JavaRDD<BaseStation> baseStationRDD = spark.read()
                .textFile(path)
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

        //基站数据存入oracle
        StatisticAnalysis.dbWrite(baseStationDF,"CELL_INFO");

    }
}
