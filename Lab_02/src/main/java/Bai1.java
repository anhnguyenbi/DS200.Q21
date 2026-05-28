import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai1 {

    public static class Step1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Set<String> stopWords = new HashSet<>();
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Đọc file stopwords.txt từ Distributed Cache
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
            // Tách các cột bằng dấu chấm phẩy ;
            String[] parts = value.toString().split(";");

            if (parts.length >= 2) {
                // Lấy cột Review (index 1), đưa về chữ thường và lọc ký tự đặc biệt
                String review = parts[1].toLowerCase().replaceAll("[^a-pà-ỹa-z0-9\\s]", "");
                String[] words = review.split("\\s+");

                for (String w : words) {
                    // Loại bỏ từ trống và từ nằm trong danh sách stopword
                    if (!w.isEmpty() && !stopWords.contains(w)) {
                        word.set(w);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class Step1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Ở Bài 1 chúng ta chỉ cần xuất ra danh sách từ đã lọc
            // Tạm thời đếm số lần xuất hiện để kiểm tra kết quả tách từ
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Bai1 <input_path> <output_path> <stopwords_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Bai 1: Preprocessing");
        job.setJarByClass(Bai1.class);
        job.setMapperClass(Step1Mapper.class);
        job.setReducerClass(Step1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Nạp file stopword vào hệ thống để Mapper sử dụng
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}