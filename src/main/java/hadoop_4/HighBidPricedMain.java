package hadoop_4;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HighBidPricedMain extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            GenericOptionsParser.printGenericCommandUsage(System.out);
            System.out.print("You should provide two arguments <inputPath> <outputPath>");
        }

        int exitCode = ToolRunner.run(new HighBidPricedMain(),
                                      args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf());

        job.setMapperClass(HighBidPricedMapper.class);
        job.setReducerClass(HighBidPricedReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setPartitionerClass(HighBidPricePartitioner.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //job.addCacheFile(new Path(args[2]).toUri());
        job.setJarByClass(HighBidPricedMain.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
