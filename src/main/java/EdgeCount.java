import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by liying on 2018/11/9.
 */
public class EdgeCount {

    private static volatile Broadcast<Map<String, Item>> ids;
    private static final ClassTag<Item> tagItem = ClassManifestFactory.classType(Item.class);
    private static final ClassTag<Double> tagDouble = ClassManifestFactory.classType(Double.class);

    public static void main(String[] args) {
        String logFile = "raw_graph_data.txt"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(logFile).cache();

        JavaRDD<String> languages = lines.flatMap(s -> Arrays.asList(s.split(";")).iterator()).distinct();

        List<String> languageList = languages.collect();

        //生成名称->Item对应Map
        Map<String, Item> languageMap = new HashMap<>();
        for (int i = 0; i < languageList.size(); i++) {
            languageMap.put(languageList.get(i), new Item(i, languageList.get(i), 1, "unknown"));
        }

        ids = sc.broadcast(languageMap);
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
        JavaPairRDD<String, Integer> pairs = languageCombinations.mapToPair(s -> new Tuple2(s, 1));

        JavaPairRDD<String, Integer> newPairs = pairs.reduceByKey((a, b) -> a + b);

        //生成edge
        JavaRDD<Edge<Double>> edges = newPairs.flatMap((FlatMapFunction<Tuple2<String, Integer>, Edge<Double>>) stringIntegerTuple2 -> {
            List<Edge<Double>> doubleEdges = new ArrayList<>();

            String[] split = stringIntegerTuple2._1().split(",");
            doubleEdges.add(new Edge<>(Long.parseLong(split[0]), Long.parseLong(split[1]), (double) stringIntegerTuple2._2()));
            doubleEdges.add(new Edge<>(Long.parseLong(split[1]), Long.parseLong(split[0]), (double) stringIntegerTuple2._2()));
            return doubleEdges.iterator();
        });

        //生成vertex
        List<Tuple2<Object, Item>> vertexList = languageMap.keySet().stream().map(key -> new Tuple2<Object, Item>(languageMap.get(key).getId(), languageMap.get(key))).collect(Collectors.toList());
        JavaRDD<Tuple2<Object, Item>> vertices = sc.parallelize(vertexList);

        Graph<Item, Double> g = Graph.apply(vertices.rdd(), edges.rdd(), new Item(0, "other", 0, "other"), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagItem, tagDouble);

        String content2 = "";
        for (Edge<Double> edge : g.edges().toJavaRDD().collect()) {
            String line = edge.srcId() + "," + edge.dstId() + "," + edge.attr().intValue() + "\n";
            content2 += line;
        }
        FileUtil.writeString("edges.txt", content2);
    }
}
