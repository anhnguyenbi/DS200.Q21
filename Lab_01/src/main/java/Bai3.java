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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai3 {

    // Mapper: đọc ratings -> key = movieId, value = userId,rating
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] tokens = line.split(",");
            if (tokens.length < 3) return;

            String userId = tokens[0].trim();
            String movieId = tokens[1].trim();
            String rating = tokens[2].trim();

            // validate rating
            try {
                Double.parseDouble(rating);
            } catch (NumberFormatException e) {
                return;
            }

            outKey.set(movieId);
            outValue.set(userId + "," + rating);
            context.write(outKey, outValue);
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, NullWritable, Text> {
        private final Map<String, String> userGenderMap = new HashMap<>();
        private final Map<String, String> movieTitleMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            String usersPath = conf.get("users.path");
            String moviesPath = conf.get("movies.path");

            if (usersPath == null || usersPath.isEmpty()) {
                throw new IOException("Thiếu tham số users.path");
            }
            if (moviesPath == null || moviesPath.isEmpty()) {
                throw new IOException("Thiếu tham số movies.path");
            }

            loadUsers(usersPath, conf);
            loadMovies(moviesPath, conf);
        }

        private void loadUsers(String usersPath, Configuration conf) throws IOException {
            Path path = new Path(usersPath);
            FileSystem fs = path.getFileSystem(conf);

            try (FSDataInputStream in = fs.open(path);
                 BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    // users.txt: UserID, Gender, Age, Occupation, Zip-code
                    String[] parts = line.split(",");
                    if (parts.length < 2) continue;

                    String userId = parts[0].trim();
                    String gender = parts[1].trim().toUpperCase(Locale.US);
                    userGenderMap.put(userId, gender);
                }
            }
        }

        private void loadMovies(String moviesPath, Configuration conf) throws IOException {
            Path path = new Path(moviesPath);
            FileSystem fs = path.getFileSystem(conf);

            try (FSDataInputStream in = fs.open(path);
                 BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    // movies.txt: MovieID, Title, Genres
                    int firstComma = line.indexOf(',');
                    int lastComma = line.lastIndexOf(',');
                    if (firstComma <= 0 || lastComma <= firstComma) continue;

                    String movieId = line.substring(0, firstComma).trim();
                    String title = line.substring(firstComma + 1, lastComma).trim();
                    movieTitleMap.put(movieId, title);
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double maleSum = 0.0;
            int maleCount = 0;
            double femaleSum = 0.0;
            int femaleCount = 0;

            for (Text v : values) {
                String[] parts = v.toString().split(",");
                if (parts.length < 2) continue;

                String userId = parts[0].trim();
                String ratingStr = parts[1].trim();

                String gender = userGenderMap.get(userId);
                if (gender == null) continue;

                double rating;
                try {
                    rating = Double.parseDouble(ratingStr);
                } catch (NumberFormatException e) {
                    continue;
                }

                if ("M".equals(gender)) {
                    maleSum += rating;
                    maleCount++;
                } else if ("F".equals(gender)) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }

            double maleAvg = maleCount > 0 ? maleSum / maleCount : 0.0;
            double femaleAvg = femaleCount > 0 ? femaleSum / femaleCount : 0.0;

            String movieId = key.toString();
            String movieTitle = movieTitleMap.getOrDefault(movieId, "Unknown Movie (" + movieId + ")");

            // Đúng yêu cầu output: MovieTitle: Male_Avg, Female_Avg
            String out = String.format(
                    Locale.US,
                    "%s: Male_Avg=%.2f, Female_Avg=%.2f",
                    movieTitle, maleAvg, femaleAvg
            );

            context.write(NullWritable.get(), new Text(out));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Bai3 <ratings_input_path> <output_path> <users_path> <movies_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("users.path", args[2]);
        conf.set("movies.path", args[3]);

        Job job = Job.getInstance(conf, "Bai3 - Rating by Gender");
        job.setJarByClass(Bai3.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}