package hadoop_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class BytesPerIPMain extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(BytesPerIPMain.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            GenericOptionsParser.printGenericCommandUsage(System.out);
            System.out.println("Usage of this application is : hadoop jar <application.jar> <packageName.mainClass> <input path> <output path>");
            System.exit(1);
        }
        logger.info("Starting application");
        int exitCode = ToolRunner.run(new BytesPerIPMain(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        logger.info("creating job");

        Configuration configuration = getConf();
        configuration.set("mapred.textoutputformat.separatorText", ",");

        Job job = Job.getInstance();
        job.setMapperClass(BytesPerIPMapper.class);
        job.setCombinerClass(BytesPerIpCombiner.class);
        job.setReducerClass(BytesPerIpReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);

        job.setJarByClass(BytesPerIPMain.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
