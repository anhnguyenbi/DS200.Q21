import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;

public class Bai7 {

    private static Dataset<Row> readCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ";")
                .csv(path);
    }

    public static int runJob(String inputDir, String outputDir) {
        SparkSession spark = SparkSession.builder()
                .appName("Bai7 - Top Product and Avg Rating by Product")
                .master("local[*]")
                .getOrCreate();

        try {
            Dataset<Row> items = readCsv(spark, inputDir + "/Order_Items.csv");
            Dataset<Row> reviews = readCsv(spark, inputDir + "/Order_Reviews.csv");
            Dataset<Row> products = readCsv(spark, inputDir + "/Products.csv");

            Dataset<Row> salesByProduct = items.groupBy("Product_ID")
                    .agg(count(col("Order_Item_ID")).alias("sales_count"));

            long topSales = salesByProduct.agg(max("sales_count")).first().getLong(0);

            Dataset<Row> topProducts = salesByProduct.filter(col("sales_count").equalTo(topSales))
                    .join(products, "Product_ID")
                    .select(
                            lit("TOP_SELLING").alias("result_type"),
                            col("Product_ID"),
                            col("Product_Category_Name"),
                            col("sales_count"),
                            lit(null).cast("double").alias("avg_rating")
                    );

            Dataset<Row> avgRatingByProduct = items.join(reviews, "Order_ID")
                    .filter(col("Review_Score").isNotNull()
                            .and(col("Review_Score").geq(1))
                            .and(col("Review_Score").leq(5)))
                    .groupBy("Product_ID")
                    .agg(avg("Review_Score").alias("avg_rating"))
                    .join(products, "Product_ID")
                    .select(
                            lit("AVG_RATING").alias("result_type"),
                            col("Product_ID"),
                            col("Product_Category_Name"),
                            lit(null).cast("long").alias("sales_count"),
                            col("avg_rating")
                    );

            Dataset<Row> result = topProducts.unionByName(avgRatingByProduct)
                    .withColumn("sort_order", when(col("result_type").equalTo("TOP_SELLING"), lit(0)).otherwise(lit(1)))
                    .orderBy(col("sort_order"), col("sales_count").desc(), col("avg_rating").desc())
                    .drop("sort_order");

            result.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .option("header", "true")
                    .csv(outputDir);

            System.out.println("Bai7 completed. Top sales count: " + topSales);
            return 0;
        } finally {
            spark.stop();
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: Bai7 <input_dir> <output_dir>");
            System.exit(1);
        }
        System.exit(runJob(args[0], args[1]));
    }
}
