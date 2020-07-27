package sparkplay;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple4;
import scala.Tuple6;

/**
 * Hello world!
 */
public final class App {
    private App() {
    }

    /**
     * Says hello to the world.
     *
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("sparkplay").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> data = sc.textFile("src/main/resources/data.csv");

            JavaRDD<Tuple6<String, String, String, String, String, String>> rowData = data.map(f -> {
                String[] row = f.split(",");
                Tuple6<String, String, String, String, String, String> tuple =
                new Tuple6<>(row[0], row[1], row[2], row[3], row[4], row[5]);
                return tuple;
            });

            rowData = rowData.filter(f-> f._2().startsWith("F"));

            System.out.format("-------- Word Count - %s --------", rowData.count());
        }
    }
}
