import java.io.IOException;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import java.io.Serializable;
import java.util.stream.Stream;

public class Main {
    
    public static Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }
    
    public static class Movie implements Serializable {
        @Getter @Setter private Integer movieId;
        @Getter @Setter private String title;
        @Getter @Setter private String genre;
        public Movie()
        {};
        public Movie(Integer movieId, String title, String genere ) {
            super();
            this.movieId = movieId;
            this.title = title;
            this.genre = genere;
        }
        /*
        public Integer getMovieId() {
            return movieId;
        }
        public void setMovieId(Integer movieId ) {
            this.movieId = movieId ;
        }
        public String getTitle() {
            return title;
        }
        public void setTitle(String title ) {
            this.title = title;
        }
        public String getGenere() {
            return genre;
        }
        public void setGenere(String genere ) {
            this.genre = genere;
        }
        */
        public static Movie parseRating(String str ) {
            String[] fields = str.split(",");
            if ( fields . length != 3) {
                System.out.println("The elements are ::");
                Stream.of( fields ).forEach(System. out ::println);
                throw new IllegalArgumentException("Each line must contain 3 fields while the current line has ::" + fields.length );
            }
            Integer movieId = Integer.parseInt( fields [0]);
            String title = fields [1].trim();
            String genere = fields [2].trim();
            return new Movie(movieId, title, genere);
        }
    }
    
    public static void main(String[] args) throws IOException {
        
        SparkSession sparkSession =SparkSession.builder()
                .master("local")
                .appName("Spark Session Example")
                .enableHiveSupport()
                .getOrCreate();
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        
        Dataset<Row> csv = sparkSession.read().format("csv").option("header","true").load("nationalparks.csv");
        csv.show();
        
        //To get Dataset with only selected columns.
        List<String> columns = Arrays.asList("Name", "Location");
        Dataset<Row> csv_selected_columns = csv.selectExpr(convertListToSeq(columns));
        
        // Working with tuples.
        JavaRDD<String> stringRDD = sparkContextFromSession.parallelize(Arrays.asList("Hello Spark", "Hello Java"));
        List<Tuple2<String, Integer>> flatMapToPair = stringRDD.flatMapToPair(s -> Arrays.asList(s.split(" "))
                                                               .stream()
                                                               .map(token -> new Tuple2<String, Integer>(token, 1))
                                                               .collect(Collectors.toList())
                                                               .iterator())
                                                               .foldByKey(0,(v1, v2) -> v1+v2)
                                                               .collect();

        // Initialize spark session.
        /* Not required.
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]");  // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        */

        // Get the data to JavaRDD<String> from csv file.
        JavaRDD<String> data = sparkContext.textFile("nationalparks.csv");

        // Print the number of rows in the csv file.
        System.out.println("Number of lines in file = " + data.count());
        System.out.println("Yohoo all good!!!");

        // Get the national parks data into JavaRDD<Schema>.
        JavaRDD<Schema> rdd_records = sparkContext.textFile("nationalparks.csv").map(
                (Function<String, Schema>) line -> {
                    // Here you can use JSON
                    // Gson gson = new Gson();
                    // gson.fromJson(line, Record.class);
                    String[] fields = line.split(",");
                    Schema sd = new Schema(fields[0], fields[1], fields[2], fields[3]);
                    return sd;
                });

        // Print the names of the national parks in the csv file.
        rdd_records.foreach(item -> {
            System.out.println("* " + item.name);
        });

        // Get only the rows with year_established >= 1900.
        /*
        Job is failing here and exiting. Reason: Integer.parseInt(item.getYear_established()). TODO: Resolve this.
        JavaRDD<Schema> rdd_year_established_filter = rdd_records.filter(item -> Integer.parseInt(item.getYear_established()) >= 1990);
        rdd_year_established_filter.foreach(item -> {
            System.out.println("* " + item.getName() + " * " + item.getYear_established());
        });
        */
        // This is how we need to do so we don't get exceptions we get for the above commented code.
        JavaRDD<Movie> moviesRDD = sparkSession.read().textFile("movies.csv")
                .javaRDD().filter(str -> !(null == str))
                .filter(str -> !(str.length()==0))
                .filter(str -> !str.contains("movieId"))
                .map(str -> Movie.parseRating(str));
        //moviesRDD.foreach(m -> System.out.println(m));

        moviesRDD.foreach(item -> {
            System.out.println("* " + item.getMovieId() + " * " + item.getTitle());
        });

        List<Order> orders = Arrays.asList(new Order("123", "John"), new Order("456", "Smith"), new Order("789", "Samuel"));
        List<LineItem> items = Arrays.asList(new LineItem("123", "Pen"), new LineItem("456", "Pencil"));

        JavaRDD<Order> rddOrders = sparkContext.parallelize(orders);
        JavaRDD<LineItem> rddLineItems = sparkContext.parallelize(items);

        JavaPairRDD<String, Order> pairRddOrders = rddOrders.mapToPair(x -> {
            return new Tuple2<String, Order>(x.getName(), x);
        });

        JavaPairRDD<String, LineItem> pairRddLineItems = rddLineItems.mapToPair(x -> {
            return new Tuple2<String, LineItem>(x.getName(), x);
        });

        JavaPairRDD<String, Tuple2<Order, LineItem>> joinedRdd = pairRddOrders.join(pairRddLineItems);


        joinedRdd.foreach(x -> {
            Tuple2<Order, LineItem> orderAndLineItems = x._2();
            System.out.println("Order= " + orderAndLineItems._1().getName() + " "+ orderAndLineItems._1().getLocation()+ " LineItems= " + orderAndLineItems._2().getName() + " "+ orderAndLineItems._2().getLocation());
            //System.out.println("LineItems= " + orderAndLineItems._2().getName() + " "+ orderAndLineItems._2().getLocation());

            });

        JavaRDD<Integer>  convertAllToOne = joinedRdd.map(x -> 1);

        System.out.println("Ok lets check whether all the values are converted to 1 or not:");

        convertAllToOne.foreach(x ->
        {
            System.out.println(x + "\n");

        });
        
        JavaRDD<Tuple2<Order, LineItem>> getRows = joinedRdd.map(x -> x._2());
        getRows.foreach(x ->
        {
            System.out.println(x);

        });
    }
}
