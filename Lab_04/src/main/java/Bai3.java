import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;

public class Bai3 {

    private static Dataset<Row> readCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ";")
                .csv(path);
    }

    public static int runJob(String inputDir, String outputDir) {
        SparkSession spark = SparkSession.builder()
                .appName("Bai3 - Orders by Country")
                .master("local[*]")
                .getOrCreate();

        try {
            Dataset<Row> orders = readCsv(spark, inputDir + "/Orders.csv");
            Dataset<Row> customers = readCsv(spark, inputDir + "/Customer_List.csv");

            Dataset<Row> result = orders.join(customers, "Customer_Trx_ID")
                    .groupBy("Customer_Country")
                    .agg(countDistinct("Order_ID").alias("order_count"))
                    .orderBy(col("order_count").desc());

            result.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .option("header", "true")
                    .csv(outputDir);

            System.out.println("Bai3 completed. Results saved to " + outputDir);
            return 0;
        } finally {
            spark.stop();
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: Bai3 <input_dir> <output_dir>");
            System.exit(1);
        }
        System.exit(runJob(args[0], args[1]));
    }
}
