import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class Bai5 {

    private static Dataset<Row> readCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ";")
                .csv(path);
    }

    public static int runJob(String inputDir, String outputDir) {
        SparkSession spark = SparkSession.builder()
                .appName("Bai5 - Review Score Statistics")
                .master("local[*]")
                .getOrCreate();

        try {
            Dataset<Row> reviews = readCsv(spark, inputDir + "/Order_Reviews.csv");

            Dataset<Row> validReviews = reviews.filter(
                    col("Review_Score").isNotNull()
                            .and(col("Review_Score").geq(1))
                            .and(col("Review_Score").leq(5))
            );

            Dataset<Row> byScore = validReviews.groupBy("Review_Score")
                    .agg(
                            count(col("Review_Score")).alias("review_count"),
                            avg(col("Review_Score")).alias("avg_score")
                    )
                    .orderBy(col("Review_Score"));

            byScore.coalesce(1)
                    .write()
                    .mode("overwrite")
                    .option("header", "true")
                    .csv(outputDir);

            System.out.println("Bai5 completed. Results saved to " + outputDir);
            return 0;
        } finally {
            spark.stop();
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: Bai5 <input_dir> <output_dir>");
            System.exit(1);
        }
        System.exit(runJob(args[0], args[1]));
    }
}
