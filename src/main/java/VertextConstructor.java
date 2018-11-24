import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by liying on 2018/11/9.
 */
public class VertextConstructor {
    public static void main(String[] args){
        String logFile = "raw_graph_data"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(logFile).cache();

        JavaPairRDD<String, Integer> counts = lines
                .flatMap(s -> Arrays.asList(s.split(";")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        List<Tuple2<String,Integer>> c = counts.collect();
        //生成名称->Item对应Map
        Map<String, Item> languageMap = new HashMap<>();
        for (int i = 0; i < c.size(); i++) {
            String line=i+","+c.get(i)._1()+c.get(i)._2();
//            languageMap.put(c.get(i)._1, new Item((long)i,c.get(i)._1,c.get(i)._2,"Language"));
        }


//        String content="";
//        for (Tuple2<Object, Item> tuple:c){
//            String line =tuple._1()+","+tuple._2().getName()+","+tuple._2().getCount()+","+tuple._2().getType()+"\n";
//            content+=line;
//        }
//        FileUtil.writeString("nodes.txt",content);

    }


    public static String getType(String content){
            return "";
    }
}
