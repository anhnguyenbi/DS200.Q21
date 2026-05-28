import java.io.*;
import java.util.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai2 {

    public static class StatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Set<String> stopWords = new HashSet<>();
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try (BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        stopWords.add(line.trim().toLowerCase());
                    }
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(";");
            if (parts.length >= 5) {
                // Thống kê Từ (Review)
                String review = parts[1].toLowerCase().replaceAll("[^a-z0-9\\sà-ỹ]", "");
                for (String w : review.split("\\s+")) {
                    if (!stopWords.contains(w) && !w.isEmpty()) {
                        word.set("WORD_" + w);
                        context.write(word, one);
                    }
                }
                // Thống kê Aspect (Cột 3 - index 2)
                context.write(new Text("ASPECT_" + parts[2].trim()), one);
                // Thống kê Category (Cột 4 - index 3)
                context.write(new Text("CATEGORY_" + parts[3].trim()), one);
            }
        }
    }

    public static class StatReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();

            // Lọc: Nếu là từ vựng thì phải > 500 lần, nếu là nhãn Aspect/Category thì in hết
            if (key.toString().startsWith("WORD_")) {
                if (sum > 500) context.write(key, new IntWritable(sum));
            } else {
                context.write(key, new IntWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai 2: Statistics");
        job.setJarByClass(Bai2.class);
        job.setMapperClass(StatMapper.class);
        job.setReducerClass(StatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.addCacheFile(new Path(args[2]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}