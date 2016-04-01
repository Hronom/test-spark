package com.github.hronom.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import scala.Tuple2;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws InterruptedException {
//        SparkConf conf = new SparkConf()
//            .setAppName("Spark user-activity")
//            .setMaster("local[2]")
//            .set("spark.driver.host", "localhost");
//        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkConf conf = new SparkConf().setAppName("Spark user-activity").setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Duration.apply(1000));

        // Здесь могла быть загрузка из файла sc.textFile("users-visits.log");
        // Но я решил применить к входным данным метод parallelize(); Для наглядности

//        List<String> visitsLog = Arrays.asList(
//            "user_id:0000, habrahabr.ru",
//            "user_id:0001, habrahabr.ru",
//            "user_id:0002, habrahabr.ru",
//            "user_id:0000, abc.ru",
//            "user_id:0000, yxz.ru",
//            "user_id:0002, qwe.ru",
//            "user_id:0002, zxc.ru",
//            "user_id:0001, qwe.ru"
//            // итд, дофантазируйте дальше сами :)
//        );

        JavaDStream<String> customReceiverStream = ssc.receiverStream(new JavaCustomReceiver());
        JavaDStream<String> words = customReceiverStream.flatMap(
            new FlatMapFunction<String, String>() {
                public Iterable<String> call(String line) {
                    return Arrays.asList(line.split(" "));
                }
            }
        );
        words.print();
        ssc.start();
        ssc.awaitTermination();

//        JavaDStream<String> customReceiverStream = sc.receiverStream(new JavaCustomReceiver(host, port));

//        JavaRDD<String> visits = sc.parallelize(visitsLog);
//
//        // Из каждой записи делаем пары: ключ (user_id), значение (1 - как факт посещения)
//        // (user_id:0000 : 1)
//        JavaPairRDD<String, Integer> pairs =
//            visits.mapToPair(new PairFunction<String, String, Integer>() {
//                @Override
//                public scala.Tuple2<String, Integer> call(String str) throws Exception {
//                    String[] kv = str.split(",");
//                    return new Tuple2<>(kv[0], 1);
//                }
//            });
//
//        // Суммируем факты посещений для каждого user_id
//        JavaPairRDD<String, Integer> counts =
//            pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
//                @Override
//                public Integer call(Integer a, Integer b) throws Exception {
//                    return a + b;
//                }
//            });
//
//        // Сиртируем по Value и возвращаем первые 10 запсисей.
//        List<Tuple2<String, Integer>> top10 = counts.takeOrdered(10, new CountComparator());
//
//        Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
//        Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");
//
//        JavaRDD<Map<String, ?>> javaRDD = sc.parallelize(ImmutableList.of(numbers, airports));
//        JavaEsSpark.saveToEs(javaRDD, "spark/docs");
//
//        System.out.println(top10);

        Thread.sleep(100000000l);
    }

    // Такие дела, компаратор должен быть Serializable. Иначе (в случае анонимного класса), получим исключение
    // SparkException: Task not serializable
    // http://stackoverflow.com/questions/29301704/apache-spark-simple-word-count-gets-sparkexception-task-not-serializable
    public static class CountComparator
        implements Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return o2._2() - o1._2();
        }
    }
}
