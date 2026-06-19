import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.year;

public class Bai6 {

    private static Dataset<Row> readCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ";")
                .csv(path);
    }

    public static int runJob(String inputDir, String outputDir) {
        SparkSession spark = SparkSession.builder()
                .appName("Bai6 - Revenue 2024 by Category")
                .master("local[*]")
                .getOrCreate();

        try {
            Dataset<Row> orders = readCsv(spark, inputDir + "/Orders.csv");
            Dataset<Row> items = readCsv(spark, inputDir + "/Order_Items.csv");
            Dataset<Row> products = readCsv(spark, inputDir + "/Products.csv");

            Dataset<Row> result = items.join(orders, "Order_ID")
                    .join(products, "Product_ID")
                    .withColumn("purchase_ts", to_timestamp(col("Order_Purchase_Timestamp"), "yyyy-MM-dd HH:mm"))
                    .filter(year(col("purchase_ts")).equalTo(2024))
                    .withColumn("revenue", col("Price").plus(col("Freight_Value")))
                    .groupBy("Product_Category_Name")
                    .agg(sum("revenue").alias("total_revenue"))
                    .orderBy(col("total_revenue").desc());

            result.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .option("header", "true")
                    .csv(outputDir);

            System.out.println("Bai6 completed. Results saved to " + outputDir);
            return 0;
        } finally {
            spark.stop();
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: Bai6 <input_dir> <output_dir>");
            System.exit(1);
        }
        System.exit(runJob(args[0], args[1]));
    }
}
