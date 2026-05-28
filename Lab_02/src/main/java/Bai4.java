import java.io.*;
import java.util.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai4 {

    public static class TopWordsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try (BufferedReader br = new BufferedReader(new FileReader("stopwords.txt"))) {
                    String line;
                    while ((line = br.readLine()) != null) stopWords.add(line.trim().toLowerCase());
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(";");
            if (parts.length >= 4) {
                String category = parts[3].trim().toUpperCase();
                String review = parts[1].toLowerCase().replaceAll("[^a-z0-9\\sà-ỹ]", "");
                for (String w : review.split("\\s+")) {
                    if (!w.isEmpty() && !stopWords.contains(w)) {
                        context.write(new Text(category), new Text(w));
                    }
                }
            }
        }
    }

    public static class TopWordsReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> wordCount = new HashMap<>();
            for (Text val : values) {
                String w = val.toString();
                wordCount.put(w, wordCount.getOrDefault(w, 0) + 1);
            }

            // Sắp xếp để lấy Top 5
            List<Map.Entry<String, Integer>> list = new ArrayList<>(wordCount.entrySet());
            list.sort((a, b) -> b.getValue().compareTo(a.getValue()));

            StringBuilder result = new StringBuilder("\n");
            for (int i = 0; i < Math.min(5, list.size()); i++) {
                result.append("  - ").append(list.get(i).getKey())
                        .append(": ").append(list.get(i).getValue()).append("\n");
            }
            context.write(key, new Text(result.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai 4: Top 5 Words per Category");
        job.setJarByClass(Bai4.class);
        job.setMapperClass(TopWordsMapper.class);
        job.setReducerClass(TopWordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new Path(args[2]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}