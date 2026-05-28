import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai1 {

    private static final int MIN_RATINGS = 5;

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text movieId = new Text();
        private final DoubleWritable rating = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] tokens = line.split(",");
            if (tokens.length < 3) {
                return;
            }

            try {
                movieId.set(tokens[1].trim());
                rating.set(Double.parseDouble(tokens[2].trim()));
                context.write(movieId, rating);
            } catch (NumberFormatException ignored) {
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {
        private final Map<String, String> movieMap = new HashMap<>();
        private String maxMovie = null;
        private double maxRating = -1.0;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String moviesPath = conf.get("movies.path");
            if (moviesPath == null || moviesPath.isEmpty()) {
                throw new IOException("Thieu tham so movies.path");
            }

            Path path = new Path(moviesPath);
            FileSystem fs = path.getFileSystem(conf);

            try (FSDataInputStream in = fs.open(path);
                 BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        continue;
                    }

                    int firstComma = line.indexOf(',');
                    int lastComma = line.lastIndexOf(',');
                    if (firstComma <= 0 || lastComma <= firstComma) {
                        continue;
                    }

                    String id = line.substring(0, firstComma).trim();
                    String title = line.substring(firstComma + 1, lastComma).trim();
                    movieMap.put(id, title);
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count == 0) {
                return;
            }

            double avg = sum / count;
            String movieId = key.toString();
            String movieTitle = movieMap.getOrDefault(movieId, "Unknown Movie (" + movieId + ")");

            String line = String.format(
                    Locale.US,
                    "%s AverageRating: %.2f (TotalRatings: %d)",
                    movieTitle, avg, count
            );
            context.write(NullWritable.get(), new Text(line));

            if (count >= MIN_RATINGS && avg > maxRating) {
                maxRating = avg;
                maxMovie = movieTitle;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (maxMovie != null) {
                String finalLine = String.format(
                        Locale.US,
                        "%s is the highest rated movie with an average rating of %.2f among movies with at least %d ratings.",
                        maxMovie, maxRating, MIN_RATINGS
                );
                context.write(NullWritable.get(), new Text(finalLine));
            } else {
                context.write(
                        NullWritable.get(),
                        new Text("No movie has at least " + MIN_RATINGS + " ratings.")
                );
            }
        }
    }

    public static int runJob(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Bai1 <ratings_input_path> <output_path> <movies_path>");
            return 1;
        }

        Configuration conf = new Configuration();
        conf.set("movies.path", args[2]);

        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Bai1 - Movie Average Rating");
        job.setJarByClass(Bai1.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(runJob(args));
    }
}
