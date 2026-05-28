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

public class Bai5 {

    public static class OccupationMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Map<String, String> userOccupationMap = new HashMap<>();
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String usersPath = conf.get("users.path");
            if (usersPath == null || usersPath.isEmpty()) {
                throw new IOException("Thieu tham so users.path");
            }

            Path path = new Path(usersPath);
            FileSystem fs = path.getFileSystem(conf);

            try (FSDataInputStream in = fs.open(path);
                 BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        continue;
                    }

                    String[] parts = line.split(",");
                    if (parts.length < 4) {
                        continue;
                    }

                    userOccupationMap.put(parts[0].trim(), parts[3].trim());
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");
            if (parts.length < 3) {
                return;
            }

            String userId = parts[0].trim();
            String occupationId = userOccupationMap.get(userId);
            if (occupationId == null) {
                return;
            }

            try {
                double rating = Double.parseDouble(parts[2].trim());
                outKey.set(occupationId);
                outValue.set(String.format(Locale.US, "%.4f,1", rating));
                context.write(outKey, outValue);
            } catch (NumberFormatException ignored) {
            }
        }
    }

    public static class OccupationReducer extends Reducer<Text, Text, NullWritable, Text> {
        private final Map<String, String> occupationNameMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String occupationPath = conf.get("occupation.path");
            if (occupationPath == null || occupationPath.isEmpty()) {
                throw new IOException("Thieu tham so occupation.path");
            }

            Path path = new Path(occupationPath);
            FileSystem fs = path.getFileSystem(conf);

            try (FSDataInputStream in = fs.open(path);
                 BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        continue;
                    }

                    String[] parts = line.split(",");
                    if (parts.length < 2) {
                        continue;
                    }

                    occupationNameMap.put(parts[0].trim(), parts[1].trim());
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                if (parts.length < 2) {
                    continue;
                }
                sum += Double.parseDouble(parts[0]);
                count += Integer.parseInt(parts[1]);
            }

            if (count == 0) {
                return;
            }

            String occupationId = key.toString();
            String occupationName = occupationNameMap.getOrDefault(
                    occupationId,
                    "OccupationID=" + occupationId
            );
            double avg = sum / count;

            String out = String.format(
                    Locale.US,
                    "%s: AvgRating=%.2f, TotalRatings=%d",
                    occupationName, avg, count
            );
            context.write(NullWritable.get(), new Text(out));
        }
    }

    public static int runJob(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Bai5 <ratings_input_path> <output_path> <users_path> <occupation_path>");
            return 1;
        }

        Configuration conf = new Configuration();
        conf.set("users.path", args[2]);
        conf.set("occupation.path", args[3]);

        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Bai5 - Rating by Occupation");
        job.setJarByClass(Bai5.class);

        job.setMapperClass(OccupationMapper.class);
        job.setReducerClass(OccupationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
