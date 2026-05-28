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

public class Bai2 {

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

    public static class GenreReducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {
        private final Map<String, String> movieGenresMap = new HashMap<>();
        private final Map<String, Double> genreSum = new HashMap<>();
        private final Map<String, Integer> genreCount = new HashMap<>();

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

                    String movieId = line.substring(0, firstComma).trim();
                    String genres = line.substring(lastComma + 1).trim();
                    movieGenresMap.put(movieId, genres);
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double movieSum = 0.0;
            int movieCount = 0;
            for (DoubleWritable value : values) {
                movieSum += value.get();
                movieCount++;
            }
            if (movieCount == 0) {
                return;
            }

            String genres = movieGenresMap.get(key.toString());
            if (genres == null || genres.isEmpty()) {
                return;
            }

            for (String genre : genres.split("\\|")) {
                genre = genre.trim();
                if (genre.isEmpty()) {
                    continue;
                }
                genreSum.put(genre, genreSum.getOrDefault(genre, 0.0) + movieSum);
                genreCount.put(genre, genreCount.getOrDefault(genre, 0) + movieCount);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Double> entry : genreSum.entrySet()) {
                String genre = entry.getKey();
                int count = genreCount.getOrDefault(genre, 0);
                if (count == 0) {
                    continue;
                }

                double avg = entry.getValue() / count;
                String out = String.format(Locale.US, "%s: %.2f (TotalRatings: %d)", genre, avg, count);
                context.write(NullWritable.get(), new Text(out));
            }
        }
    }

    public static int runJob(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Bai2 <ratings_input_path> <output_path> <movies_path>");
            return 1;
        }

        Configuration conf = new Configuration();
        conf.set("movies.path", args[2]);

        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Bai2 - Genre Average Rating");
        job.setJarByClass(Bai2.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(GenreReducer.class);
        job.setNumReduceTasks(1);

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
