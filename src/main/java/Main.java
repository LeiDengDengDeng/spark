import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by liying on 2018/11/4.
 */
public class Main {

    private static final ClassTag<Integer> tagInteger = ClassManifestFactory.classType( Integer.class );
    private static final ClassTag<String> tagString = ClassManifestFactory.classType( String.class );
    private static final ClassTag<Object> tagObject = ClassManifestFactory.classType( Object.class );
    private static final ClassTag<Double> tagDouble = ClassManifestFactory.classType( Double.class );
    private static final ClassTag<Tuple2<Object, Double>> tagTuple2 = ClassManifestFactory.classType( Tuple2.class );
    private static final ClassTag<Tuple2<Boolean, Double>> tagTuple2Boolean = ClassManifestFactory.classType( Tuple2.class );

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Graph short path").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(conf);


        JavaRDD<Tuple2<Object, String>> vertexs = ctx.parallelize(
                Arrays.asList(
                        new Tuple2<Object, String>(1L, "a"),
                        new Tuple2<Object, String>(2L, "b"),
                        new Tuple2<Object, String>(3L, "c"),
                        new Tuple2<Object, String>(4L, "d"),
                        new Tuple2<Object, String>(5L, "e")
                )
        );


        JavaRDD<Edge<Double>> edges = ctx.parallelize(Arrays.asList(
                new Edge<Double>(1L, 2L, 10.0),
                 new Edge<Double>(2L, 3L, 20.0),
                new Edge<Double>(2L, 4L, 30.0),
                new Edge<Double>(4L, 5L, 80.0),
                new Edge<Double>(1L, 5L, 3.0),
                new Edge<Double>(1L, 4L, 30.0),
                 new Edge<Double>(1L, 4L, 30.0)
                )
        );


        Graph<String, Double> g = Graph.apply(vertexs.rdd(), edges.rdd(), "aa", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagString, tagDouble);

        //图的属性操作
        System.out.println("*************************************************************");
        System.out.println("属性演示");
        System.out.println("*************************************************************");

        g.vertices().toJavaRDD().collect().forEach(System.out::println);


    }
}
