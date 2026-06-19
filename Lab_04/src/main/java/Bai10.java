import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.sum;

public class Bai10 {

    private static Dataset<Row> readCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ";")
                .csv(path);
    }

    public static int runJob(String inputDir, String outputDir) {
        SparkSession spark = SparkSession.builder()
                .appName("Bai10 - Seller Ranking")
                .master("local[*]")
                .getOrCreate();

        try {
            Dataset<Row> items = readCsv(spark, inputDir + "/Order_Items.csv");

            Dataset<Row> result = items.withColumn("revenue", col("Price").plus(col("Freight_Value")))
                    .groupBy("Seller_ID")
                    .agg(
                            sum("revenue").alias("total_revenue"),
                            countDistinct("Order_ID").alias("order_count")
                    )
                    .orderBy(col("total_revenue").desc(), col("order_count").desc());

            result.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .option("header", "true")
                    .csv(outputDir);

            System.out.println("Bai10 completed. Results saved to " + outputDir);
            return 0;
        } finally {
            spark.stop();
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: Bai10 <input_dir> <output_dir>");
            System.exit(1);
        }
        System.exit(runJob(args[0], args[1]));
    }
}
