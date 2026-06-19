import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

public class Bai1 {

    private static Dataset<Row> readCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ";")
                .csv(path);
    }

    public static int runJob(String inputDir, String outputDir) {
        SparkSession spark = SparkSession.builder()
                .appName("Bai1 - Read CSV and Infer Schema")
                .master("local[*]")
                .getOrCreate();

        try {
            String[][] datasets = {
                    {"Orders.csv", inputDir + "/Orders.csv"},
                    {"Customer_List.csv", inputDir + "/Customer_List.csv"},
                    {"Order_Items.csv", inputDir + "/Order_Items.csv"},
                    {"Products.csv", inputDir + "/Products.csv"},
                    {"Order_Reviews.csv", inputDir + "/Order_Reviews.csv"}
            };

            java.util.List<Row> schemaRows = new java.util.ArrayList<>();
            for (String[] dataset : datasets) {
                Dataset<Row> df = readCsv(spark, dataset[1]);
                long rowCount = df.count();
                for (StructField field : df.schema().fields()) {
                    schemaRows.add(org.apache.spark.sql.RowFactory.create(
                            dataset[0],
                            field.name(),
                            field.dataType().simpleString(),
                            rowCount
                    ));
                }
            }

            Dataset<Row> result = spark.createDataFrame(
                    schemaRows,
                    new org.apache.spark.sql.types.StructType()
                            .add("dataset", "string")
                            .add("column_name", "string")
                            .add("data_type", "string")
                            .add("row_count", "long")
            );

            result.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .option("header", "true")
                    .csv(outputDir);

            System.out.println("Bai1 completed. Schema saved to " + outputDir);
            return 0;
        } finally {
            spark.stop();
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: Bai1 <input_dir> <output_dir>");
            System.exit(1);
        }
        System.exit(runJob(args[0], args[1]));
    }
}
