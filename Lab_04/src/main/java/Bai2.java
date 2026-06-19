import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Bai2 {

    private static Dataset<Row> readCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ";")
                .csv(path);
    }

    public static int runJob(String inputDir, String outputDir) {
        SparkSession spark = SparkSession.builder()
                .appName("Bai2 - Total Orders, Customers, Sellers")
                .master("local[*]")
                .getOrCreate();

        try {
            Dataset<Row> orders = readCsv(spark, inputDir + "/Orders.csv");
            Dataset<Row> customers = readCsv(spark, inputDir + "/Customer_List.csv");
            Dataset<Row> orderItems = readCsv(spark, inputDir + "/Order_Items.csv");

            long totalOrders = orders.select(col("Order_ID")).distinct().count();
            long totalCustomers = customers.count();
            long totalSellers = orderItems.select(col("Seller_ID")).distinct().count();

            java.util.List<Row> rows = java.util.Arrays.asList(
                    org.apache.spark.sql.RowFactory.create("Total Orders", totalOrders),
                    org.apache.spark.sql.RowFactory.create("Total Customers", totalCustomers),
                    org.apache.spark.sql.RowFactory.create("Total Sellers", totalSellers)
            );

            Dataset<Row> result = spark.createDataFrame(
                    rows,
                    new org.apache.spark.sql.types.StructType()
                            .add("metric", "string")
                            .add("value", "long")
            );

            result.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .option("header", "true")
                    .csv(outputDir);

            System.out.println("Total Orders: " + totalOrders);
            System.out.println("Total Customers: " + totalCustomers);
            System.out.println("Total Sellers: " + totalSellers);
            return 0;
        } finally {
            spark.stop();
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: Bai2 <input_dir> <output_dir>");
            System.exit(1);
        }
        System.exit(runJob(args[0], args[1]));
    }
}
