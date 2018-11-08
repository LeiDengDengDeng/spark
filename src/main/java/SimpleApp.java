
/**
 * MIT.
 * Author: wangxiaolei(王小雷).
 * Date:17-2-7.
 * Project:SparkJavaIdea.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

import java.util.*;
import java.util.stream.Collectors;

public class SimpleApp {
    private static final ClassTag<Integer> tagInteger = ClassManifestFactory.classType(Integer.class);
    private static final ClassTag<Item> tagString = ClassManifestFactory.classType(Item.class);
    private static final ClassTag<Object> tagObject = ClassManifestFactory.classType(Object.class);
    private static final ClassTag<Double> tagDouble = ClassManifestFactory.classType(Double.class);

    private static volatile Broadcast<Map<String, Long>> ids;

    public static void main(String[] args){
        String logFile = "data.txt"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();


        JavaRDD<String> lines = sc.textFile(logFile).cache();

        JavaRDD<String> languages = lines.flatMap(s -> Arrays.asList(s.split(";")).iterator()).distinct();

        List<String> languageList = languages.collect();
        Map<String, Long> languageMap = new HashMap<>();
        for (int i = 0; i < languageList.size(); i++) {
            languageMap.put(languageList.get(i), (long) i);
        }
        // 生成id
//        JavaPairRDD<String,Long> zipWithIndexRDD = languages.zipWithUniqueId();

//        zipWithIndexRDD.collect().get(0).

//        System.out.println(languageMap);
        ids = sc.broadcast(languageMap);


//        System.out.println(zipWithIndexRDD.collect());


        JavaRDD<String> languageCombinations = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(";");
                List<String> combinations = new ArrayList<>();
                for (int i = 0; i < split.length - 1; i++) {
                    for (int j = i + 1; j < split.length; j++) {
//                        String combination = split[i].compareTo(split[j]) > 0 ? split[i] + "-" + split[j] : split[j] + "-" + split[i];

                        Long id1 = ids.value().get(split[i]);
                        Long id2 = ids.value().get(split[j]);
                        String combination = id1 < id2 ? id1 + "," + id2 : id2 + "," + id1;
                        combinations.add(combination);
                    }
                }
                return combinations.iterator();
            }
        });
        System.out.println(languageCombinations.collect());

//        Dataset<Row> df = spark.createDataFrame(lines.flatMap(s -> Arrays.asList(s.split(",")).iterator()),String.class);
//        System.out.println(df.collect());


        JavaPairRDD<String, Integer> pairs = languageCombinations.mapToPair(s -> new Tuple2(s, 1));
        System.out.println(pairs.reduceByKey((a, b) -> a + b).collect());


//        long numAs = lines.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) { return s.contains("java"); }
//        }).count();
//
//        long numBs = lines.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) { return s.contains("php"); }
//        }).count();
//
//        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);


//        List<Edge<Double>> list = new ArrayList<>();
//        for (Tuple2<String, Integer> tuple2 : pairs.reduceByKey((a, b) -> a + b).collect()) {
//            String[] split = tuple2._1().split(",");
//            list.add(new Edge<Double>(Long.parseLong(split[0]), Long.parseLong(split[1]), (double) tuple2._2()));
//        }
//        JavaRDD<Edge<Double>> edges = sc.parallelize(list);

        //vertex
//        List<Tuple2<Object, Item>> vertexList = languageMap.keySet().stream().map(key -> new Tuple2<Object, Item>(languageMap.get(key), new Item(key, "language"))).collect(Collectors.toList());
//        JavaRDD<Tuple2<Object, Item>> vertices = sc.parallelize(vertexList);

//        Graph<Item, Double> g = Graph.apply(vertices.rdd(), edges.rdd(), new Item("1","1"), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagString, tagDouble);
//
//        g.vertices().toJavaRDD().collect().forEach(System.out::println);
//        g.vertices().toJavaRDD().collect().forEach(v-> System.out.println(v._2.name+","+v._2.type));
//        g.edges().toJavaRDD().collect().forEach(System.out::println);
//        System.out.println(g.edges().count());
//        g.triplets().toJavaRDD().collect().forEach(System.out::println);


        sc.stop();
    }

}

