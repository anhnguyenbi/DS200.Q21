import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.year;

public class Bai4 {

    private static Dataset<Row> readCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ";")
                .csv(path);
    }

    public static int runJob(String inputDir, String outputDir) {
        SparkSession spark = SparkSession.builder()
                .appName("Bai4 - Orders by Year and Month")
                .master("local[*]")
                .getOrCreate();

        try {
            Dataset<Row> orders = readCsv(spark, inputDir + "/Orders.csv");

            Dataset<Row> result = orders
                    .withColumn("purchase_ts", to_timestamp(col("Order_Purchase_Timestamp"), "yyyy-MM-dd HH:mm"))
                    .withColumn("order_year", year(col("purchase_ts")))
                    .withColumn("order_month", month(col("purchase_ts")))
                    .filter(col("order_year").isNotNull())
                    .groupBy("order_year", "order_month")
                    .agg(count(col("Order_ID")).alias("order_count"))
                    .orderBy(col("order_year").asc(), col("order_month").desc());

            result.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .option("header", "true")
                    .csv(outputDir);

            System.out.println("Bai4 completed. Results saved to " + outputDir);
            return 0;
        } finally {
            spark.stop();
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: Bai4 <input_dir> <output_dir>");
            System.exit(1);
        }
        System.exit(runJob(args[0], args[1]));
    }
}
