package hadoop_1;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class LongestWordCounter {

    public static void main(String[] args) throws IOException {
        Job job = Job.getInstance();
        job.setJarByClass(LongestWordCounter.class);
        job.setMapperClass(LongestWordMapper.class);
        job.setReducerClass(LongestWordReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path("/input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output.txt"));

        try {
            System.exit(job.waitForCompletion(true)? 0:1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
