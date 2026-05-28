import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
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

public class Bai6 {

    public static class YearMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");
            if (parts.length < 4) {
                return;
            }

            try {
                double rating = Double.parseDouble(parts[2].trim());
                long timestamp = Long.parseLong(parts[3].trim());
                int year = ZonedDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneOffset.UTC).getYear();

                outKey.set(String.valueOf(year));
                outValue.set(String.format(Locale.US, "%.4f,1", rating));
                context.write(outKey, outValue);
            } catch (NumberFormatException ignored) {
            }
        }
    }

    public static class YearReducer extends Reducer<Text, Text, NullWritable, Text> {
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

            double avg = sum / count;
            String out = String.format(
                    Locale.US,
                    "Year %s: AvgRating=%.2f, TotalRatings=%d",
                    key.toString(), avg, count
            );
            context.write(NullWritable.get(), new Text(out));
        }
    }

    public static int runJob(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Bai6 <ratings_input_path> <output_path>");
            return 1;
        }

        Configuration conf = new Configuration();

        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Bai6 - Rating by Year");
        job.setJarByClass(Bai6.class);

        job.setMapperClass(YearMapper.class);
        job.setReducerClass(YearReducer.class);

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
