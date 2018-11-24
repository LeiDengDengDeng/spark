import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.*;
import org.apache.spark.graphx.lib.ShortestPaths;
import org.apache.spark.storage.StorageLevel;
import scala.Function1;
import scala.Tuple2;
import scala.collection.immutable.Map;
import scala.collection.mutable.ArraySeq;
import scala.reflect.ClassTag;

import java.io.FileWriter;
import java.io.IOException;


/**
 * Created by liying on 2018/11/4.
 */
public class GraphX {



    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Graph");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        ClassTag<Integer> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);


        //创建一个初始的RDD
        JavaRDD<String> lines = ctx.textFile(args[0]+"data.txt");
        JavaRDD<String> vertex = ctx.textFile(args[0]+"variables.txt");

        JavaPairRDD<String,Integer> pairs=lines.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s,1));
        JavaPairRDD red=pairs.reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer+integer2);
        int res=red.filter((Function<Tuple2, Boolean>) tuple2 -> (int)tuple2._2()>1).collect().size();

        JavaPairRDD red2=pairs.distinct().mapToPair((PairFunction<Tuple2<String, Integer>, String, Integer>) stringIntegerTuple2 -> {
            String[] split=stringIntegerTuple2._1().split(",");
            String s1=split[0];
            String s2=split[1];
            long v1=Long.parseLong(s1.substring(2));
            long v2=Long.parseLong(s2.substring(2));
            if(v1>v2){
                return new Tuple2<>(s2+","+s1,1);
            }
            return stringIntegerTuple2;
        }).reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer+integer2);
        int res2=red2.filter((Function<Tuple2, Boolean>) tuple2 -> (int)tuple2._2()>1).collect().size();

        //生成边
        JavaRDD<Edge<Integer>> edges=lines.map((Function<String, Edge<Integer>>) s -> {
            String[] splits=s.split(",");
            String v1=splits[0].substring(2);
            String v2=splits[1].substring(2);
            return new Edge(Long.parseLong(v1),Long.parseLong(v2),1);
        });


        Graph<String, Integer> graph = Graph.fromEdges(edges.rdd(),"",StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),stringTag,intTag);
        int des=Integer.parseInt(vertex.collect().get(1).substring(2));
        int src=Integer.parseInt(vertex.collect().get(0).substring(2));
        ArraySeq<Object> seq=new ArraySeq<>(1);
        seq.update(0,des);
        Tuple2<Object,Map<Object,Object>> tuple2=ShortestPaths.run(graph, seq,intTag).vertices().toJavaRDD().filter(v1->(long)v1._1()==src).collect().get(0);
        int l=(int)tuple2._2().get(des).get();

        try {
            //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
            FileWriter writer = new FileWriter(args[0]+"MF1832090.txt",false);
            writer.write(res2+"\n"+res+"\n"+l);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }




    }
}
