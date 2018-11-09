import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.storage.StorageLevel;
import scala.*;
import scala.Function2;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

import java.lang.Boolean;
import java.lang.Double;
import java.lang.Long;
import java.util.*;

/**
 * Created by liying on 2018/11/5.
 */
public class LanguageGraph {

    private static final ClassTag<Item> tagItem = ClassManifestFactory.classType( Item.class );
    private static final ClassTag<Double> tagDouble = ClassManifestFactory.classType( Double.class );


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Graph short path").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        //创建一个初始的RDD
        JavaRDD<String> lines = ctx.textFile("rateLinks.txt");
        JavaRDD<String> nodes = ctx.textFile("all-nodes.txt");
        //对初始的RDD进行transformation操作，也就是一些计算操作


        //生成点
        JavaRDD<Tuple2<Object, Item>> vertices=nodes.map((Function<String, Tuple2<Object, Item>>) s -> {
            String[] split=s.split(",");
            Item item=new Item(Long.parseLong(split[0]),split[1],Integer.parseInt(split[2]),split[3]);
            return new Tuple2<>(Long.parseLong(split[0]),item);
        })
                .filter(new Function<Tuple2<Object, Item>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Object, Item> objectItemTuple2) throws Exception {
                return objectItemTuple2._2().getCount()>9000;
            }
        });
        //生成边
        JavaRDD<Edge<Double>> edges=lines.map((Function<String, Edge<Double>>) s -> {
            String[] splits=s.split(",");
            return new Edge<>(Long.parseLong(splits[0]),Long.parseLong(splits[1]),Double.parseDouble(splits[2]));
        })
                .filter(new Function<Edge<Double>, Boolean>() {
            @Override
            public Boolean call(Edge<Double> doubleEdge) throws Exception {
                return doubleEdge.attr()>1.9 ;
            }
        });

        Graph<Item, Double> graph = Graph.apply(vertices.rdd(), edges.rdd(), new Item(0,"other",0,"other"), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagItem, tagDouble);

        graph=graph.subgraph(new AbsFunc1(),new AbsFunc2());
        //图的属性操作
        System.out.println("*************************************************************");
        System.out.println("属性演示");
        System.out.println("*************************************************************");


        graph.vertices().toJavaRDD().collect().forEach(System.out::print);


        String content="";
        for (Tuple2<Object, Item> tuple:graph.vertices().toJavaRDD().collect()){

            String line =tuple._1()+","+tuple._2().getName()+","+tuple._2().getCount()+","+tuple._2().getType()+"\n";
            content+=line;
        }
        FileUtil.writeString("vertices.txt",content);

        String content2="";
        for (Edge<Double> edge: graph.edges().toJavaRDD().collect()) {
            String line= edge.srcId()+","+edge.dstId()+","+edge.attr()+"\n";
            content2+=line;
        }
//        graph.ops().stronglyConnectedComponents(5).vertices().toJavaRDD().collect().forEach(System.out::print);
        FileUtil.writeString("links.txt",content2);

    }
}
