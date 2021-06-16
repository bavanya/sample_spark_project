import java.io.IOException;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Main {
    public static void main(String[] args) throws IOException {

        // Initialize spark session.
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]");  // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Get the data to JavaRDD<String> from csv file.
        JavaRDD<String> data = sparkContext.textFile("nationalparks.csv");

        // Print the number of rows in the csv file.
        System.out.println("Number of lines in file = " + data.count());
        System.out.println("Yayyy all good!!!");

        // Get the national parks data into JavaRDD<Schema>.
        JavaRDD<Schema> rdd_records = sparkContext.textFile("nationalparks.csv").map(
                (Function<String, Schema>) line -> {
                    // Here you can use JSON
                    // Gson gson = new Gson();
                    // gson.fromJson(line, Record.class);
                    String[] fields = line.split(",");
                    Schema sd = new Schema(fields[0], fields[1], Integer.parseInt(fields[2]), Float.parseFloat(fields[3]));
                    return sd;
                });

        // Print the names of the national parks in the csv file.
        rdd_records.foreach(item -> {
            System.out.println("* " + item.name);
        });
        
        // Get only the rows with year_established >= 1900.
        JavaRDD<Schema> rdd_year_established_filter = rdd_records.filter(item -> item.getYear_established()>=1990);
        rdd_year_established_filter.foreach(item -> {
            System.out.println("* " + item.getName() + "*" + item.getYear_established());
        });
    }
}
