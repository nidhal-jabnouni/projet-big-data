package tn.insat.projet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CategoryCount {

    public static class CatMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text category = new Text();
        private final static LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            // Skip header line
            if (key.get() == 0 && value.toString().contains("INSPECTION CATEGORY")) {
                return;
            }
            String[] cols = value.toString().split("\t");
            String cat = cols[8]; // “INSPECTION CATEGORY”
            if (!cat.isEmpty()) {
                category.set(cat);
                ctx.write(category, ONE);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> vals, Context ctx)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : vals) sum += v.get();
            ctx.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CategoryCount <in> <out>");
            System.exit(1);
        }
        Job job = Job.getInstance(new Configuration(), "Inspections By Category");
        job.setJarByClass(CategoryCount.class);

        job.setMapperClass(CatMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
