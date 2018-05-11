import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkCoreTest
{

    /*数据情况
        a 1
        b 2
        c 3
        d 4
        e 5*/
    public static void main( String[] args )
    {
        String filepath="E://SparkCoreTest";

        SparkConf conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd=sc.textFile(filepath);
        // transform
//        testSparkCoreApiMap(rdd);
//        testSparkCoreApiFilter(rdd);
//        testSparkCoreApiFlatMap(rdd);
//
//        testSparkCoreApiUnion(rdd);
//         testSparkCoreApiDistinct(rdd);
//         testSparkCoreApiMaptoPair(rdd);
//        testSparkCoreApiGroupByKey(rdd);
//        testSparkCoreApiReduceByKey(rdd);
//        // action
//        testSparkCoreApiReduce(rdd);

        JavaRDD<String> rdd2=sc.textFile("E://test.txt");
        JavaPairRDD t1 = rdd.mapToPair(s -> new Tuple2<>(s.split(" ")[0],s.split(" ")[1]).swap());
        JavaPairRDD t2 = rdd2.mapToPair(s -> new Tuple2<>(s.split(" ")[0],s.split(" ")[1]));
        JavaPairRDD t3 = t1.leftOuterJoin(t2).sortByKey();
        t3.collect().forEach(s -> System.out.println(s));
    }


    /**
     * Map主要是对数据进行处理，不进行数据集的增减
     *
     * @param rdd 原始数据
     */
    private static void testSparkCoreApiMap(JavaRDD<String> rdd){
        //打印所有数据
        JavaRDD<String> lines =rdd.map(s -> s);
        List list = lines.collect();
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }
        // 字符长度
        JavaRDD<Integer> lineLengths =rdd.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println("所有字符总长度：" + totalLength);
    }

    /**
     * filter主要是过滤数据的功能
     * 本案例实现：过滤含有a的那行数据
     */
    private static void testSparkCoreApiFilter(JavaRDD<String> rdd){
        JavaRDD<String> logData1=rdd.filter(new Function<String,Boolean>(){
            public Boolean call(String s){
                return (s.split(" "))[0].equals("a");
            }
        });
        List list = logData1.collect();
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }
    }


    /*
     *
     *
     * flatMap  用户行转列
     * 本案例实现：打印所有的字符
     *
     *
     */
    private static void testSparkCoreApiFlatMap(JavaRDD<String> rdd){
//        JavaRDD<String> words=rdd.flatMap(
//                new FlatMapFunction<String, String>() {
//                    public Iterable<String> call(String s) throws Exception {
//                        return Arrays.asList(s.split(" "));
//                    }
//                }
//        );
        JavaRDD<String> words=rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        List list = words.collect();
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }
    }



    /**
     * testSparkCoreApiUnion
     * 合并两个RDD
     * @param rdd
     */
    private static void testSparkCoreApiUnion(JavaRDD<String> rdd){
        JavaRDD<String> unionRdd=rdd.union(rdd);
        unionRdd.foreach(new VoidFunction<String>(){
            public void call(String lines){
                System.out.println(lines);
            }
        });
    }


    /**
     * testSparkCoreApiDistinct Test
     * 对RDD去重
     * @param rdd
     */
    private static void testSparkCoreApiDistinct(JavaRDD<String> rdd){
        JavaRDD<String> unionRdd=rdd.union(rdd).distinct();
        unionRdd.foreach(new VoidFunction<String>(){
            public void call(String lines){
                System.out.println(lines);
            }
        });
    }


    /**
     * testSparkCoreApiMaptoPair Test
     * 把RDD映射为键值对类型的数据
     * @param rdd
     */
    private static void testSparkCoreApiMaptoPair(JavaRDD<String> rdd){
        JavaPairRDD<String, Integer> pairRdd=rdd.mapToPair(new PairFunction<String,String,Integer>(){
            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                String[] st=t.split(" ");
                return new Tuple2(st[0], st[1]);
            }

        });

        pairRdd.foreach(new VoidFunction<Tuple2<String, Integer>>(){
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._2());
            }
        });

    }



    /**
     * testSparkCoreApiGroupByKey Test
     * 对键值对类型的数据进行按键值合并
     * @param rdd
     */

    private static void testSparkCoreApiGroupByKey(JavaRDD<String> rdd){

        JavaPairRDD<String, Integer> pairRdd=rdd.mapToPair(new PairFunction<String,String,Integer>(){
            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                String[] st=t.split(" ");
                return new Tuple2(st[0], Integer.valueOf(st[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> pairrdd2= pairRdd.union(pairRdd).groupByKey();
        pairrdd2.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>(){
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                Iterable<Integer> iter = t._2();
                for (Integer integer : iter) {
                    System.out.println(integer);
                }
            }
        });
    }


    /**
     * testSparkCoreApiReduceByKey
     * 对键值对进行按键相同的对值进行操作
     * @param rdd
     */
    private static void testSparkCoreApiReduceByKey(JavaRDD<String> rdd){

        JavaPairRDD<String, Integer> pairRdd=rdd.mapToPair(new PairFunction<String,String,Integer>(){
            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                String[] st=t.split(" ");
                return new Tuple2(st[0], Integer.valueOf(st[1]));
            }
        });

        JavaPairRDD<String, Integer> pairrdd2 =pairRdd.union(pairRdd).reduceByKey(
                new Function2<Integer,Integer,Integer>(){
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                }
        ).sortByKey() ;
        pairrdd2.foreach(new VoidFunction<Tuple2<String, Integer>>(){
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._2());

            }
        });
    }


    /**
     * testSparkCoreApiReduce
     * 对RDD进行递归调用
     * @param rdd
     */
    private static void testSparkCoreApiReduce(JavaRDD<String> rdd){
        //由于原数据是String，需要转为Integer才能进行reduce递归
        JavaRDD<Integer> rdd1=rdd.map(new Function<String,Integer>(){
            @Override
            public Integer call(String v1) throws Exception {
            // TODO Auto-generated method stub
                return Integer.valueOf(v1.split(" ")[1]);
            }
        });

        Integer a= rdd1.reduce(new Function2<Integer,Integer,Integer>(){
            @Override
            public Integer call(Integer v1,Integer v2) throws Exception {
                return v1+v2;
            }
        });
        System.out.println(a);

    }



}