
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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Function1;
import scala.Tuple2;
import scala.math.Ordering;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;
import scala.runtime.BoxedUnit;
import scala.tools.cmd.gen.AnyVals;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class SimpleApp {
    private static final ClassTag<Integer> tagInteger = ClassManifestFactory.classType(Integer.class);
    private static final ClassTag<Item> tagString = ClassManifestFactory.classType(Item.class);
    private static final ClassTag<Object> tagObject = ClassManifestFactory.classType(Object.class);
    private static final ClassTag<Double> tagDouble = ClassManifestFactory.classType(Double.class);

    private static volatile Broadcast<Map<String, Item>> ids;

    public static void main(String[] args){
        String logFile = "data1.txt"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();


        JavaRDD<String> lines = sc.textFile(logFile).cache();

        JavaRDD<String> languages = lines.flatMap(s -> Arrays.asList(s.split(";")).iterator()).distinct();

        List<String> languageList = languages.collect();

        //生成名称->Item对应Map
        Map<String, Item> languageMap = new HashMap<>();
        for (int i = 0; i < languageList.size(); i++) {
            languageMap.put(languageList.get(i), new Item(i,languageList.get(i),1,"language"));
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
                        Long id1 = ids.value().get(split[i]).getId();
                        Long id2 = ids.value().get(split[j]).getId();
                        String combination = id1 < id2 ? id1 + "," + id2 : id2 + "," + id1;
                        combinations.add(combination);
                    }
                }
                return combinations.iterator();
            }
        });
//        System.out.println(languageCombinations.collect());

//        Dataset<Row> df = spark.createDataFrame(lines.flatMap(s -> Arrays.asList(s.split(",")).iterator()),String.class);
//        System.out.println(df.collect());


        JavaPairRDD<String, Integer> pairs = languageCombinations.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> newPairs=pairs.reduceByKey((a, b) -> a + b);
        System.out.println(newPairs.collect());
        //生成edges

        JavaRDD<Edge<Double>> originEdges=pairs.flatMap((FlatMapFunction<Tuple2<String, Integer>, Edge<Double>>) stringIntegerTuple2 -> {
            List<Edge<Double>> doubleEdges = new ArrayList<>();

            String[] split=stringIntegerTuple2._1().split(",");
            doubleEdges.add(new Edge<>(Long.parseLong(split[0]), Long.parseLong(split[1]), (double) stringIntegerTuple2._2()));
            doubleEdges.add(new Edge<>(Long.parseLong(split[1]), Long.parseLong(split[0]), (double) stringIntegerTuple2._2()));
            return doubleEdges.iterator();
        });






        //生成edges
        JavaRDD<Edge<Double>> edges=newPairs.flatMap((FlatMapFunction<Tuple2<String, Integer>, Edge<Double>>) stringIntegerTuple2 -> {
            List<Edge<Double>> doubleEdges = new ArrayList<>();

            String[] split=stringIntegerTuple2._1().split(",");
            doubleEdges.add(new Edge<>(Long.parseLong(split[0]), Long.parseLong(split[1]), (double) stringIntegerTuple2._2()));
            doubleEdges.add(new Edge<>(Long.parseLong(split[1]), Long.parseLong(split[0]), (double) stringIntegerTuple2._2()));
            return doubleEdges.iterator();
        }).filter(new Function<Edge<Double>, Boolean>() {
            @Override
            public Boolean call(Edge<Double> doubleEdge) throws Exception {
                return doubleEdge.attr()>2000;
            }
        });

//            @Override
//            public Edge<Double> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                String[] split=stringIntegerTuple2._1().split(",");
//                return new Edge<Double>(Long.parseLong(split[0]), Long.parseLong(split[1]), (double) stringIntegerTuple2._2());
//            }
//        });

        //生成vertex
        List<Tuple2<Object, Item>> vertexList = languageMap.keySet().stream().map(key -> new Tuple2<Object, Item>(languageMap.get(key).getId(), languageMap.get(key))).collect(Collectors.toList());
        JavaRDD<Tuple2<Object, Item>> vertices = sc.parallelize(vertexList);

        //生成图
        Graph<Item, Double> g = Graph.apply(vertices.rdd(), edges.rdd(), new Item(0,"other",0,"other"), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagString, tagDouble);

        Graph<Item, Double> g2 = Graph.apply(vertices.rdd(), originEdges.rdd(), new Item(0,"other",0,"other"), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagString, tagDouble);







//        g.vertices().toJavaRDD().collect().forEach(System.out::print);
//        g.edges().toJavaRDD().collect().forEach(System.out::print);
//        System.out.println();
//        System.out.println(g.ops().triangleCount().vertices().toJavaRDD().collect());
//        System.out.println();
//        System.out.println(g.ops().pageRank(0.001,0.15).vertices().toJavaRDD().collect());
//        String content="";
//        for (Tuple2<Object, Item> tuple:g.vertices().toJavaRDD().collect()){
//
//            String line =tuple._1()+","+tuple._2().getName()+","+tuple._2().getCount()+","+tuple._2().getType()+"\n";
//            content+=line;
//        }
//        FileUtil.writeString("nodes.txt",content);
//
//        String content2="";
//        for (Edge<Double> edge: g.edges().toJavaRDD().collect()) {
//            String line= edge.srcId()+","+edge.dstId()+","+edge.attr().intValue()+"\n";
//            content2+=line;
//        }
//        FileUtil.writeString("links.txt",content2);

        String content="";
        for (Tuple2<Object, Object> tuple:g2.ops().degrees().toJavaRDD().collect()){

            String line =tuple._1()+","+tuple._2()+"\n";
            content+=line;
        }
        FileUtil.writeString("degrees.txt",content);







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
//
//        //vertex
//        List<Tuple2<Object, Item>> vertexList = languageMap.keySet().stream().map(key -> new Tuple2<Object, Item>(languageMap.get(key), new Item(key, "language"))).collect(Collectors.toList());
//        JavaRDD<Tuple2<Object, Item>> vertices = sc.parallelize(vertexList);
//
//        Graph<Item, Double> g = Graph.apply(vertices.rdd(), edges.rdd(), new Item("1","1"), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagString, tagDouble);

//        vertices.saveAsTextFile("vertices.txt");
//        g.vertices().toJavaRDD().collect().forEach(System.out::println);
//        g.vertices().toJavaRDD().collect().forEach(v-> System.out.println(v._2.name+","+v._2.type));
//        g.edges().toJavaRDD().collect().forEach(System.out::println);
//        System.out.println(g.edges().count());
//        g.triplets().toJavaRDD().collect().forEach(System.out::println);



        sc.stop();
    }

}

